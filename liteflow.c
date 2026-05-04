/*
 * liteflow - a tiny dynamic-DAG runner with LLM-in-the-loop control
 *
 * Unlike static DAG runners (Airflow, Make), liteflow treats the LLM as a
 * first-class citizen of the control plane:
 *   - on_failure planner hooks can patch, retry, or extend the graph
 *   - decision nodes let the LLM choose among declared branches
 *   - every state transition is appended to events.jsonl for full audit
 *
 * The mutation grammar is a deliberately tiny verb set:
 *   RETRY                 - try again unchanged
 *   PATCH {field, value}  - modify one field, retry
 *   INSERT_BEFORE {task}  - inject a remediation task, then retry
 *   ABORT                 - give up
 *
 * Anything else from the planner is logged and treated as ABORT.
 *
 * Build:  cc -std=c11 -O2 -Wall -Wextra -o liteflow liteflow.c
 * Usage:  liteflow run workflow.yaml [--logdir DIR]
 *         liteflow validate workflow.yaml
 *         liteflow replay <event_log.jsonl>
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <threads.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#  include <direct.h>
#  include <io.h>
#  define popen  _popen
#  define pclose _pclose
#  define MKDIR(p) _mkdir(p)
#  define PATH_SEP '\\'
#else
#  include <unistd.h>
#  include <sys/wait.h>
#  define MKDIR(p) mkdir((p), 0755)
#  define PATH_SEP '/'
#endif

/* ============================================================
 *  Generic utilities
 * ============================================================ */

static void die(const char *msg) {
    fprintf(stderr, "liteflow: %s\n", msg);
    exit(1);
}
static void *xcalloc(size_t n, size_t sz) {
    void *p = calloc(n, sz);
    if (!p) die("out of memory");
    return p;
}
static void *xrealloc(void *p, size_t n) {
    void *q = realloc(p, n);
    if (!q) die("out of memory");
    return q;
}
static char *xstrdup(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s);
    char *p = xcalloc(n + 1, 1);
    memcpy(p, s, n);
    return p;
}

static char *str_trim(char *s) {
    if (!s) return s;
    while (*s && isspace((unsigned char)*s)) s++;
    char *e = s + strlen(s);
    while (e > s && isspace((unsigned char)e[-1])) e--;
    *e = '\0';
    return s;
}

static char *str_unquote(char *s) {
    if (!s) return s;
    size_t n = strlen(s);
    if (n >= 2 && ((s[0] == '"' && s[n-1] == '"') ||
                   (s[0] == '\'' && s[n-1] == '\''))) {
        s[n-1] = '\0';
        return s + 1;
    }
    return s;
}

/* JSON-escape into heap buffer */
static char *json_escape(const char *s) {
    if (!s) s = "";
    size_t cap = strlen(s) * 2 + 16;
    char *out = xcalloc(cap, 1);
    size_t j = 0;
    for (size_t i = 0; s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        if (j + 8 >= cap) { cap *= 2; out = xrealloc(out, cap); }
        switch (c) {
            case '"':  out[j++]='\\'; out[j++]='"'; break;
            case '\\': out[j++]='\\'; out[j++]='\\'; break;
            case '\n': out[j++]='\\'; out[j++]='n'; break;
            case '\r': out[j++]='\\'; out[j++]='r'; break;
            case '\t': out[j++]='\\'; out[j++]='t'; break;
            default:
                if (c < 0x20) j += snprintf(out + j, cap - j, "\\u%04x", c);
                else out[j++] = (char)c;
        }
    }
    out[j] = '\0';
    return out;
}

/* Single-quote escape for POSIX shell */
static char *sh_q(const char *s) {
    if (!s) s = "";
    size_t cap = strlen(s) * 4 + 4;
    char *out = xcalloc(cap, 1);
    size_t j = 0;
    out[j++] = '\'';
    for (size_t i = 0; s[i]; i++) {
        if (s[i] == '\'') {
            out[j++]='\''; out[j++]='\\'; out[j++]='\''; out[j++]='\'';
        } else out[j++] = s[i];
    }
    out[j++] = '\''; out[j] = '\0';
    return out;
}

/* Read whole file to heap string */
static char *slurp(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "liteflow: cannot open %s: %s\n", path, strerror(errno));
        exit(1);
    }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = xcalloc((size_t)sz + 1, 1);
    if (sz > 0) {
        size_t got = fread(buf, 1, (size_t)sz, f);
        buf[got] = '\0';
    }
    fclose(f);
    return buf;
}

/* dynamic string list */
typedef struct { char **items; size_t n, cap; } StrList;
static void sl_push(StrList *l, char *s) {
    if (l->n == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 4;
        l->items = xrealloc(l->items, l->cap * sizeof(char *));
    }
    l->items[l->n++] = s;
}
static void sl_free(StrList *l) {
    for (size_t i = 0; i < l->n; i++) free(l->items[i]);
    free(l->items);
    l->items = NULL; l->n = l->cap = 0;
}

/* ============================================================
 *  YAML subset parser
 *
 * Supports: 2-space indentation, mappings, block sequences (`- ...`),
 * flow sequences (`[a, b]`), quoted strings, # comments. That's it.
 * ============================================================ */

static void clean_line(char *line) {
    bool sq = false, dq = false;
    char *p = line;
    while (*p) {
        if (*p == '"' && !sq) dq = !dq;
        else if (*p == '\'' && !dq) sq = !sq;
        else if (*p == '#' && !sq && !dq) { *p = '\0'; break; }
        p++;
    }
    char *e = line + strlen(line);
    while (e > line && (e[-1]=='\n'||e[-1]=='\r'||e[-1]==' '||e[-1]=='\t')) e--;
    *e = '\0';
}

static int line_indent(const char *line) {
    int n = 0;
    while (line[n] == ' ') n++;
    if (line[n] == '\t') die("tabs not supported in YAML");
    return n;
}

static void parse_flow_seq(const char *src, StrList *out) {
    const char *p = src;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '[') die("expected '['");
    p++;
    while (*p && *p != ']') {
        while (*p && (isspace((unsigned char)*p) || *p == ',')) p++;
        if (*p == ']' || !*p) break;
        char buf[1024]; size_t bi = 0; char qc = 0;
        if (*p == '"' || *p == '\'') { qc = *p; p++; }
        while (*p && bi + 1 < sizeof buf) {
            if (qc) { if (*p == qc) { p++; break; } buf[bi++] = *p++; }
            else { if (*p == ',' || *p == ']') break; buf[bi++] = *p++; }
        }
        buf[bi] = '\0';
        sl_push(out, xstrdup(str_trim(buf)));
    }
}

/* ============================================================
 *  Workflow data model
 * ============================================================ */

typedef enum {
    TASK_SHELL, TASK_LLM, TASK_FILE_READ, TASK_FILE_WRITE, TASK_DECISION
} TaskType;

typedef enum {
    ST_PENDING = 0, ST_READY, ST_RUNNING, ST_DONE, ST_FAILED, ST_SKIPPED, ST_GATED_OUT
} TaskState;

typedef struct {
    char *model;        /* planner LLM model */
    int   budget;       /* max mutations applied to this task; default 3 */
    int   used;         /* mutations consumed */
} FailurePolicy;

typedef struct {
    char *source;          /* "yaml" or "planner" */
    char *parent_mutation; /* mutation id that created me, if planner */
    char *parent_task;     /* task that triggered the mutation */
} Origin;

typedef struct Task {
    char     *id;
    TaskType  type;
    /* shell */
    char     *cmd;
    /* llm worker */
    char     *model;
    char     *prompt;
    /* file_read / file_write */
    char     *path;
    char     *content;
    /* decision */
    StrList   branches;    /* candidate task ids the LLM may pick from */
    char     *chosen;      /* set after decision runs; matches `when` */
    /* gating */
    char     *when;        /* "<decision_id>": only run if that decision chose me */
    StrList   deps;
    /* fault tolerance */
    int       retries;
    FailurePolicy on_failure;
    /* provenance */
    Origin    origin;
    /* runtime */
    TaskState state;
    int       exit_code;
} Task;

typedef struct Workflow {
    char  *name;
    Task **tasks;
    size_t n_tasks, cap_tasks;
} Workflow;

static Task *wf_find(Workflow *wf, const char *id) {
    for (size_t i = 0; i < wf->n_tasks; i++)
        if (strcmp(wf->tasks[i]->id, id) == 0) return wf->tasks[i];
    return NULL;
}

static void wf_add(Workflow *wf, Task *t) {
    if (wf->n_tasks == wf->cap_tasks) {
        wf->cap_tasks = wf->cap_tasks ? wf->cap_tasks * 2 : 8;
        wf->tasks = xrealloc(wf->tasks, wf->cap_tasks * sizeof(Task *));
    }
    wf->tasks[wf->n_tasks++] = t;
}

static TaskType parse_task_type(const char *s) {
    if (!s) die("task missing 'type'");
    if (!strcmp(s, "shell"))      return TASK_SHELL;
    if (!strcmp(s, "llm"))        return TASK_LLM;
    if (!strcmp(s, "file_read"))  return TASK_FILE_READ;
    if (!strcmp(s, "file_write")) return TASK_FILE_WRITE;
    if (!strcmp(s, "decision"))   return TASK_DECISION;
    fprintf(stderr, "liteflow: unknown task type '%s'\n", s);
    exit(1);
}

static void apply_field(Task *t, const char *k, char *v) {
    if (!strcmp(k, "id"))           t->id = xstrdup(str_unquote(v));
    else if (!strcmp(k, "type"))    t->type = parse_task_type(str_unquote(v));
    else if (!strcmp(k, "cmd"))     t->cmd = xstrdup(str_unquote(v));
    else if (!strcmp(k, "model"))   t->model = xstrdup(str_unquote(v));
    else if (!strcmp(k, "prompt"))  t->prompt = xstrdup(str_unquote(v));
    else if (!strcmp(k, "path"))    t->path = xstrdup(str_unquote(v));
    else if (!strcmp(k, "content")) t->content = xstrdup(str_unquote(v));
    else if (!strcmp(k, "when"))    t->when = xstrdup(str_unquote(v));
    else if (!strcmp(k, "retries")) t->retries = atoi(v);
    else if (!strcmp(k, "depends_on")) parse_flow_seq(v, &t->deps);
    else if (!strcmp(k, "branches"))   parse_flow_seq(v, &t->branches);
}

static Workflow *parse_workflow(const char *path) {
    char *text = slurp(path);
    StrList lines = {0};
    char *p = text;
    while (*p) {
        char *ls = p;
        while (*p && *p != '\n') p++;
        if (*p == '\n') { *p = '\0'; p++; }
        clean_line(ls);
        sl_push(&lines, ls);
    }

    Workflow *wf = xcalloc(1, sizeof *wf);
    Task *cur = NULL;
    bool in_tasks = false;
    bool in_on_failure = false;

    for (size_t li = 0; li < lines.n; li++) {
        char *line = lines.items[li];
        if (!*line || line[0] == '#') continue;
        int ind = line_indent(line);
        char *c = line + ind;

        if (ind == 0) {
            in_tasks = false; in_on_failure = false; cur = NULL;
            char *colon = strchr(c, ':');
            if (!colon) continue;
            *colon = '\0';
            char *k = str_trim(c), *v = str_trim(colon + 1);
            if (!strcmp(k, "name")) wf->name = xstrdup(str_unquote(v));
            else if (!strcmp(k, "tasks")) in_tasks = true;
        }
        else if (in_tasks && ind == 2 && c[0] == '-') {
            in_on_failure = false;
            cur = xcalloc(1, sizeof *cur);
            cur->origin.source = xstrdup("yaml");
            cur->on_failure.budget = 3;
            wf_add(wf, cur);
            char *rest = str_trim(c + 1);
            if (*rest) {
                char *colon = strchr(rest, ':');
                if (colon) {
                    *colon = '\0';
                    apply_field(cur, str_trim(rest), str_trim(colon + 1));
                }
            }
        }
        else if (in_tasks && ind == 4 && cur) {
            in_on_failure = false;
            char *colon = strchr(c, ':');
            if (!colon) continue;
            *colon = '\0';
            char *k = str_trim(c), *v = str_trim(colon + 1);
            if (!strcmp(k, "on_failure") && !*v) {
                in_on_failure = true;
                if (!cur->on_failure.model)
                    cur->on_failure.model = xstrdup("gpt-4o-mini");
            } else {
                apply_field(cur, k, v);
            }
        }
        else if (in_tasks && in_on_failure && ind == 6 && cur) {
            char *colon = strchr(c, ':');
            if (!colon) continue;
            *colon = '\0';
            char *k = str_trim(c), *v = str_trim(colon + 1);
            if (!strcmp(k, "planner")) {
                free(cur->on_failure.model);
                cur->on_failure.model = xstrdup(str_unquote(v));
            } else if (!strcmp(k, "budget")) {
                cur->on_failure.budget = atoi(v);
            }
        }
    }

    if (!wf->name) wf->name = xstrdup("workflow");
    /* validate */
    for (size_t i = 0; i < wf->n_tasks; i++) {
        Task *t = wf->tasks[i];
        if (!t->id) { fprintf(stderr, "liteflow: task #%zu missing id\n", i); exit(1); }
        for (size_t j = 0; j < i; j++)
            if (!strcmp(wf->tasks[j]->id, t->id)) {
                fprintf(stderr, "liteflow: duplicate id '%s'\n", t->id); exit(1);
            }
        for (size_t k = 0; k < t->deps.n; k++)
            if (!wf_find(wf, t->deps.items[k])) {
                fprintf(stderr, "liteflow: '%s' depends on unknown '%s'\n",
                        t->id, t->deps.items[k]); exit(1);
            }
        if (t->when && !wf_find(wf, t->when)) {
            fprintf(stderr, "liteflow: '%s' has when='%s' but no such task\n",
                    t->id, t->when); exit(1);
        }
        if (t->type == TASK_DECISION) {
            for (size_t k = 0; k < t->branches.n; k++)
                if (!wf_find(wf, t->branches.items[k])) {
                    fprintf(stderr, "liteflow: decision '%s' references unknown branch '%s'\n",
                            t->id, t->branches.items[k]); exit(1);
                }
        }
    }

    free(text);
    free(lines.items);
    return wf;
}

static void free_task(Task *t) {
    free(t->id); free(t->cmd); free(t->model); free(t->prompt);
    free(t->path); free(t->content); free(t->when); free(t->chosen);
    free(t->on_failure.model);
    free(t->origin.source); free(t->origin.parent_mutation); free(t->origin.parent_task);
    sl_free(&t->branches); sl_free(&t->deps);
    free(t);
}

static void free_workflow(Workflow *wf) {
    if (!wf) return;
    free(wf->name);
    for (size_t i = 0; i < wf->n_tasks; i++) free_task(wf->tasks[i]);
    free(wf->tasks);
    free(wf);
}

/* ============================================================
 *  Logging context + event log
 * ============================================================ */

typedef struct {
    char *root;
    char *run_dir;
    FILE *events;     /* events.jsonl, append-only */
    mtx_t event_mtx;
    int   mutation_seq;
} RunCtx;

static void mkdir_p(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof tmp, "%s", path);
    size_t n = strlen(tmp);
    for (size_t i = 1; i < n; i++) {
        if (tmp[i] == '/' || tmp[i] == '\\') {
            char c = tmp[i]; tmp[i] = '\0'; MKDIR(tmp); tmp[i] = c;
        }
    }
    MKDIR(tmp);
}

static void run_paths(RunCtx *c, const char *task_id,
                      char *out, char *err, char *st, size_t cap) {
    snprintf(out, cap, "%s%c%s.out", c->run_dir, PATH_SEP, task_id);
    snprintf(err, cap, "%s%c%s.err", c->run_dir, PATH_SEP, task_id);
    snprintf(st,  cap, "%s%c%s.status", c->run_dir, PATH_SEP, task_id);
}

static RunCtx *make_run_ctx(const char *root, const char *wf_name) {
    RunCtx *c = xcalloc(1, sizeof *c);
    c->root = xstrdup(root ? root : "logs");
    mkdir_p(c->root);
    time_t now = time(NULL);
    struct tm tmv;
#ifdef _WIN32
    localtime_s(&tmv, &now);
#else
    localtime_r(&now, &tmv);
#endif
    char ts[64]; strftime(ts, sizeof ts, "%Y-%m-%dT%H-%M-%S", &tmv);
    char buf[1024];
    snprintf(buf, sizeof buf, "%s%c%s_%s", c->root, PATH_SEP, wf_name, ts);
    c->run_dir = xstrdup(buf);
    mkdir_p(c->run_dir);

    char ep[1024]; snprintf(ep, sizeof ep, "%s%cevents.jsonl", c->run_dir, PATH_SEP);
    c->events = fopen(ep, "wb");
    if (!c->events) die("cannot open events.jsonl");
    mtx_init(&c->event_mtx, mtx_plain);
    return c;
}

static void free_run_ctx(RunCtx *c) {
    if (!c) return;
    if (c->events) fclose(c->events);
    mtx_destroy(&c->event_mtx);
    free(c->root); free(c->run_dir); free(c);
}

static void log_eventv(RunCtx *c, const char *type, const char *fmt, ...) {
    mtx_lock(&c->event_mtx);
    time_t now = time(NULL);
    struct tm tmv;
#ifdef _WIN32
    localtime_s(&tmv, &now);
#else
    localtime_r(&now, &tmv);
#endif
    char ts[32]; strftime(ts, sizeof ts, "%Y-%m-%dT%H:%M:%S", &tmv);
    fprintf(c->events, "{\"ts\":\"%s\",\"type\":\"%s\"", ts, type);
    if (fmt && *fmt) {
        fprintf(c->events, ",");
        va_list ap; va_start(ap, fmt);
        vfprintf(c->events, fmt, ap);
        va_end(ap);
    }
    fprintf(c->events, "}\n");
    fflush(c->events);
    mtx_unlock(&c->event_mtx);
}

/* ============================================================
 *  curl wrapper for OpenAI-compatible chat completions
 *
 * Returns 0 on success and writes the raw API JSON to `out_path`.
 * stderr from curl goes to `err_path`.
 * ============================================================ */

static int call_chat_completions(const char *model, const char *prompt,
                                 const char *out_path, const char *err_path,
                                 const char *body_tmp_path) {
    const char *key = getenv("OPENAI_API_KEY");
    if (!key || !*key) {
        FILE *e = fopen(err_path, "wb");
        if (e) { fputs("OPENAI_API_KEY not set\n", e); fclose(e); }
        return 1;
    }
    const char *base = getenv("OPENAI_BASE_URL");
    if (!base || !*base) base = "https://api.openai.com/v1";

    char *ep = json_escape(prompt), *em = json_escape(model);
    size_t cap = strlen(ep) + strlen(em) + 256;
    char *body = xcalloc(cap, 1);
    snprintf(body, cap,
        "{\"model\":\"%s\",\"messages\":[{\"role\":\"user\",\"content\":\"%s\"}]}",
        em, ep);
    free(ep); free(em);

    FILE *bf = fopen(body_tmp_path, "wb");
    if (!bf) { free(body); return 1; }
    fwrite(body, 1, strlen(body), bf);
    fclose(bf);
    free(body);

    char endpoint[1024];
    snprintf(endpoint, sizeof endpoint, "%s/chat/completions", base);

#ifdef _WIN32
    char cmd[8192];
    snprintf(cmd, sizeof cmd,
        "curl -sS -X POST \"%s\" -H \"Content-Type: application/json\" "
        "-H \"Authorization: Bearer %s\" --data-binary @\"%s\" > \"%s\" 2> \"%s\"",
        endpoint, key, body_tmp_path, out_path, err_path);
    int rc = system(cmd);
    remove(body_tmp_path);
    return rc;
#else
    char *qe = sh_q(endpoint), *qa = sh_q(key);
    char *qo = sh_q(out_path), *qer = sh_q(err_path), *qt = sh_q(body_tmp_path);
    char cmd[8192];
    snprintf(cmd, sizeof cmd,
        "curl -sS -X POST %s -H 'Content-Type: application/json' "
        "-H \"Authorization: Bearer $(printf %%s %s)\" "
        "--data-binary @%s >%s 2>%s",
        qe, qa, qt, qo, qer);
    int rc = system(cmd);
    remove(body_tmp_path);
    free(qe); free(qa); free(qo); free(qer); free(qt);
    if (rc == -1) return -1;
    if (WIFEXITED(rc)) return WEXITSTATUS(rc);
    return 1;
#endif
}

/* Naive extractor for choices[0].message.content from the JSON response.
 * Robust enough for well-formed OpenAI-compatible responses; not a full parser.
 * Returns heap-allocated string, or NULL on failure. */
static char *extract_assistant_text(const char *json) {
    if (!json) return NULL;
    /* find "content":" then read until matching unescaped quote */
    const char *needle = "\"content\":";
    const char *p = strstr(json, needle);
    if (!p) return NULL;
    p += strlen(needle);
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '"') return NULL;
    p++;
    size_t cap = 1024, n = 0;
    char *out = xcalloc(cap, 1);
    while (*p) {
        if (n + 4 >= cap) { cap *= 2; out = xrealloc(out, cap); }
        if (*p == '\\' && p[1]) {
            switch (p[1]) {
                case 'n': out[n++] = '\n'; break;
                case 'r': out[n++] = '\r'; break;
                case 't': out[n++] = '\t'; break;
                case '"': out[n++] = '"'; break;
                case '\\': out[n++] = '\\'; break;
                case '/': out[n++] = '/'; break;
                default: out[n++] = p[1];
            }
            p += 2;
        } else if (*p == '"') {
            out[n] = '\0';
            return out;
        } else {
            out[n++] = *p++;
        }
    }
    free(out);
    return NULL;
}

/* ============================================================
 *  Task execution (data plane)
 * ============================================================ */

static int exec_shell(Task *t, RunCtx *c) {
    char outp[1024], errp[1024], stp[1024];
    run_paths(c, t->id, outp, errp, stp, sizeof outp);
    if (!t->cmd) return 1;
    char full[8192];
#ifdef _WIN32
    snprintf(full, sizeof full, "(%s) > \"%s\" 2> \"%s\"", t->cmd, outp, errp);
    return system(full);
#else
    char *qo = sh_q(outp), *qe = sh_q(errp);
    snprintf(full, sizeof full, "( %s ) >%s 2>%s", t->cmd, qo, qe);
    free(qo); free(qe);
    int rc = system(full);
    if (rc == -1) return -1;
    if (WIFEXITED(rc)) return WEXITSTATUS(rc);
    return 1;
#endif
}

static int exec_file_read(Task *t, RunCtx *c) {
    char outp[1024], errp[1024], stp[1024];
    run_paths(c, t->id, outp, errp, stp, sizeof outp);
    if (!t->path) return 1;
    FILE *in = fopen(t->path, "rb");
    if (!in) {
        FILE *e = fopen(errp, "wb");
        if (e) { fprintf(e, "open %s: %s\n", t->path, strerror(errno)); fclose(e); }
        return 1;
    }
    FILE *o = fopen(outp, "wb");
    if (!o) { fclose(in); return 1; }
    char b[8192]; size_t r;
    while ((r = fread(b, 1, sizeof b, in)) > 0) fwrite(b, 1, r, o);
    fclose(in); fclose(o);
    FILE *e = fopen(errp, "wb"); if (e) fclose(e);
    return 0;
}

static int exec_file_write(Task *t, RunCtx *c) {
    char outp[1024], errp[1024], stp[1024];
    run_paths(c, t->id, outp, errp, stp, sizeof outp);
    if (!t->path) return 1;
    FILE *o = fopen(t->path, "wb");
    if (!o) {
        FILE *e = fopen(errp, "wb");
        if (e) { fprintf(e, "write %s: %s\n", t->path, strerror(errno)); fclose(e); }
        return 1;
    }
    const char *content = t->content ? t->content : "";
    fwrite(content, 1, strlen(content), o);
    fclose(o);
    FILE *of = fopen(outp, "wb");
    if (of) { fprintf(of, "wrote %zu bytes\n", strlen(content)); fclose(of); }
    return 0;
}

static int exec_llm(Task *t, RunCtx *c) {
    char outp[1024], errp[1024], stp[1024];
    run_paths(c, t->id, outp, errp, stp, sizeof outp);
    if (!t->prompt) return 1;
    char tmp[1024];
    snprintf(tmp, sizeof tmp, "%s%c%s.body.json", c->run_dir, PATH_SEP, t->id);
    return call_chat_completions(t->model ? t->model : "gpt-4o-mini",
                                 t->prompt, outp, errp, tmp);
}

/* Decision: ask the LLM to pick one of t->branches. Writes raw response to
 * <task_id>.out, sets t->chosen on success. */
static int exec_decision(Task *t, RunCtx *c) {
    char outp[1024], errp[1024], stp[1024];
    run_paths(c, t->id, outp, errp, stp, sizeof outp);
    if (t->branches.n == 0) {
        FILE *e = fopen(errp, "wb");
        if (e) { fputs("decision has no branches\n", e); fclose(e); }
        return 1;
    }

    /* Compose a constrained prompt: list options, ask for one of them. */
    size_t cap = (t->prompt ? strlen(t->prompt) : 0) + 256;
    for (size_t i = 0; i < t->branches.n; i++) cap += strlen(t->branches.items[i]) + 8;
    char *full = xcalloc(cap, 1);
    size_t n = 0;
    n += snprintf(full + n, cap - n,
        "You are a workflow decision node. Pick exactly one of the option ids "
        "below and respond with ONLY that id, no quotes, no explanation.\n\n"
        "Context: %s\n\nOptions:\n",
        t->prompt ? t->prompt : "(none)");
    for (size_t i = 0; i < t->branches.n; i++)
        n += snprintf(full + n, cap - n, "  - %s\n", t->branches.items[i]);

    char tmp[1024];
    snprintf(tmp, sizeof tmp, "%s%c%s.body.json", c->run_dir, PATH_SEP, t->id);
    int rc = call_chat_completions(t->model ? t->model : "gpt-4o-mini",
                                   full, outp, errp, tmp);
    free(full);
    if (rc != 0) return rc;

    char *resp = slurp(outp);
    char *pick = extract_assistant_text(resp);
    free(resp);
    if (!pick) {
        FILE *e = fopen(errp, "ab");
        if (e) { fputs("\n[decision] could not extract assistant text\n", e); fclose(e); }
        return 1;
    }
    str_trim(pick);

    /* validate that pick is one of the branches */
    bool ok = false;
    for (size_t i = 0; i < t->branches.n; i++) {
        if (!strcmp(pick, t->branches.items[i])) { ok = true; break; }
    }
    if (!ok) {
        FILE *e = fopen(errp, "ab");
        if (e) {
            fprintf(e, "\n[decision] LLM returned '%s', not one of the declared branches\n", pick);
            fclose(e);
        }
        free(pick);
        return 1;
    }
    t->chosen = pick;
    return 0;
}

static int exec_task_once(Task *t, RunCtx *c) {
    switch (t->type) {
        case TASK_SHELL:      return exec_shell(t, c);
        case TASK_LLM:        return exec_llm(t, c);
        case TASK_FILE_READ:  return exec_file_read(t, c);
        case TASK_FILE_WRITE: return exec_file_write(t, c);
        case TASK_DECISION:   return exec_decision(t, c);
    }
    return 1;
}

/* ============================================================
 *  Planner: invoked on task failure. Returns a mutation verb.
 * ============================================================ */

typedef enum {
    MUT_ABORT, MUT_RETRY, MUT_PATCH, MUT_INSERT_BEFORE
} MutationKind;

typedef struct {
    MutationKind kind;
    /* PATCH */
    char *patch_field;
    char *patch_value;
    /* INSERT_BEFORE: a fresh shell task to insert before the failing one */
    char *insert_id;
    char *insert_cmd;
} Mutation;

static void free_mutation(Mutation *m) {
    free(m->patch_field); free(m->patch_value);
    free(m->insert_id);   free(m->insert_cmd);
    memset(m, 0, sizeof *m);
}

/* Tiny JSON value extractor: returns heap string for "key":"value" string fields.
 * Returns NULL if not found or not a string. */
static char *json_str_field(const char *json, const char *key) {
    if (!json) return NULL;
    char needle[64];
    snprintf(needle, sizeof needle, "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return NULL;
    p += strlen(needle);
    while (*p && (isspace((unsigned char)*p) || *p == ':')) p++;
    if (*p != '"') return NULL;
    p++;
    size_t cap = 256, n = 0;
    char *out = xcalloc(cap, 1);
    while (*p) {
        if (n + 4 >= cap) { cap *= 2; out = xrealloc(out, cap); }
        if (*p == '\\' && p[1]) {
            switch (p[1]) {
                case 'n': out[n++]='\n'; break;
                case 't': out[n++]='\t'; break;
                case '"': out[n++]='"'; break;
                case '\\': out[n++]='\\'; break;
                default: out[n++]=p[1];
            }
            p += 2;
        } else if (*p == '"') {
            out[n] = '\0';
            return out;
        } else out[n++] = *p++;
    }
    free(out);
    return NULL;
}

static const char *PLANNER_SYS_PROMPT =
    "You are the failure planner for a deterministic DAG runtime. A task has failed. "
    "You must respond with ONE JSON object and nothing else (no prose, no fences). "
    "Schema: {\"verb\": \"RETRY\"|\"PATCH\"|\"INSERT_BEFORE\"|\"ABORT\", "
    "\"field\": <name of task field to modify, only for PATCH>, "
    "\"value\": <new value as string, only for PATCH>, "
    "\"insert_id\": <id for new shell task, only for INSERT_BEFORE>, "
    "\"insert_cmd\": <shell command for new task, only for INSERT_BEFORE>}. "
    "Only PATCH the fields: cmd, path, content, prompt, model. "
    "INSERT_BEFORE creates a shell remediation task that runs before the failing task retries. "
    "Choose ABORT if the failure is not recoverable.";

/* Read a small file's content (truncated) for inclusion in a prompt. */
static char *read_truncated(const char *path, size_t max) {
    FILE *f = fopen(path, "rb");
    if (!f) return xstrdup("");
    char *buf = xcalloc(max + 64, 1);
    size_t r = fread(buf, 1, max, f);
    if (r == max) {
        snprintf(buf + r, 64, "\n...[truncated]");
    } else buf[r] = '\0';
    fclose(f);
    return buf;
}

static Mutation invoke_planner(Task *t, RunCtx *c) {
    Mutation m = { .kind = MUT_ABORT };

    char outp[1024], errp[1024], stp[1024];
    run_paths(c, t->id, outp, errp, stp, sizeof outp);
    char *task_out = read_truncated(outp, 2000);
    char *task_err = read_truncated(errp, 2000);

    /* Describe the failing task in JSON form for the planner. */
    char *desc_id  = json_escape(t->id);
    char *desc_cmd = json_escape(t->cmd ? t->cmd : "");
    char *desc_pr  = json_escape(t->prompt ? t->prompt : "");
    char *desc_pa  = json_escape(t->path ? t->path : "");
    char *desc_out = json_escape(task_out);
    char *desc_err = json_escape(task_err);
    free(task_out); free(task_err);

    size_t cap = strlen(desc_cmd) + strlen(desc_pr) + strlen(desc_pa)
               + strlen(desc_out) + strlen(desc_err) + strlen(PLANNER_SYS_PROMPT) + 1024;
    char *prompt = xcalloc(cap, 1);
    snprintf(prompt, cap,
        "%s\n\nFailing task:\n"
        "{\"id\":\"%s\",\"type\":%d,\"cmd\":\"%s\",\"prompt\":\"%s\",\"path\":\"%s\","
        "\"exit_code\":%d}\n\n"
        "Captured stdout:\n%s\n\nCaptured stderr:\n%s\n",
        PLANNER_SYS_PROMPT, desc_id, (int)t->type, desc_cmd, desc_pr, desc_pa,
        t->exit_code, desc_out, desc_err);
    free(desc_id); free(desc_cmd); free(desc_pr); free(desc_pa);
    free(desc_out); free(desc_err);

    /* Save planner request and response in the run dir for audit. */
    char planner_out[1024], planner_err[1024], planner_body[1024];
    snprintf(planner_out, sizeof planner_out, "%s%c%s.planner.out", c->run_dir, PATH_SEP, t->id);
    snprintf(planner_err, sizeof planner_err, "%s%c%s.planner.err", c->run_dir, PATH_SEP, t->id);
    snprintf(planner_body, sizeof planner_body, "%s%c%s.planner.body.json", c->run_dir, PATH_SEP, t->id);

    int rc = call_chat_completions(
        t->on_failure.model ? t->on_failure.model : "gpt-4o-mini",
        prompt, planner_out, planner_err, planner_body);
    free(prompt);
    if (rc != 0) {
        log_eventv(c, "planner_error",
            "\"task\":\"%s\",\"reason\":\"curl_failed\",\"rc\":%d", t->id, rc);
        return m;
    }

    char *resp = slurp(planner_out);
    char *content = extract_assistant_text(resp);
    free(resp);
    if (!content) {
        log_eventv(c, "planner_error",
            "\"task\":\"%s\",\"reason\":\"no_assistant_text\"", t->id);
        return m;
    }

    /* Strip ``` fences if present. */
    char *body = content;
    if (strstr(body, "```")) {
        char *first = strchr(body, '{');
        char *last  = strrchr(body, '}');
        if (first && last && last > first) { *(last + 1) = '\0'; body = first; }
    }

    char *verb = json_str_field(body, "verb");
    if (!verb) {
        log_eventv(c, "planner_error",
            "\"task\":\"%s\",\"reason\":\"no_verb\"", t->id);
        free(content);
        return m;
    }

    if (!strcmp(verb, "RETRY")) {
        m.kind = MUT_RETRY;
    } else if (!strcmp(verb, "ABORT")) {
        m.kind = MUT_ABORT;
    } else if (!strcmp(verb, "PATCH")) {
        m.patch_field = json_str_field(body, "field");
        m.patch_value = json_str_field(body, "value");
        if (m.patch_field && m.patch_value) m.kind = MUT_PATCH;
    } else if (!strcmp(verb, "INSERT_BEFORE")) {
        m.insert_id  = json_str_field(body, "insert_id");
        m.insert_cmd = json_str_field(body, "insert_cmd");
        if (m.insert_id && m.insert_cmd) m.kind = MUT_INSERT_BEFORE;
    }

    free(verb);
    free(content);
    return m;
}

/* Apply a mutation to the workflow. Returns true if the failing task
 * should be re-queued for execution; false if it stays failed. */
static bool apply_mutation(Workflow *wf, Task *t, Mutation *m, RunCtx *c) {
    char mid[64];
    snprintf(mid, sizeof mid, "mut-%d", ++c->mutation_seq);

    switch (m->kind) {
        case MUT_ABORT:
            log_eventv(c, "mutation_applied",
                "\"id\":\"%s\",\"verb\":\"ABORT\",\"task\":\"%s\"", mid, t->id);
            return false;

        case MUT_RETRY:
            log_eventv(c, "mutation_applied",
                "\"id\":\"%s\",\"verb\":\"RETRY\",\"task\":\"%s\"", mid, t->id);
            t->state = ST_PENDING;
            return true;

        case MUT_PATCH: {
            /* apply patch: only known safe fields */
            char *f = m->patch_field;
            char *v = m->patch_value;
            if (!strcmp(f, "cmd"))         { free(t->cmd);    t->cmd = xstrdup(v); }
            else if (!strcmp(f, "path"))   { free(t->path);   t->path = xstrdup(v); }
            else if (!strcmp(f, "content")){ free(t->content); t->content = xstrdup(v); }
            else if (!strcmp(f, "prompt")) { free(t->prompt); t->prompt = xstrdup(v); }
            else if (!strcmp(f, "model"))  { free(t->model);  t->model = xstrdup(v); }
            else {
                log_eventv(c, "mutation_rejected",
                    "\"id\":\"%s\",\"verb\":\"PATCH\",\"task\":\"%s\",\"reason\":\"field_not_allowed\",\"field\":\"%s\"",
                    mid, t->id, f);
                return false;
            }
            char *vesc = json_escape(v);
            log_eventv(c, "mutation_applied",
                "\"id\":\"%s\",\"verb\":\"PATCH\",\"task\":\"%s\",\"field\":\"%s\",\"value\":\"%s\"",
                mid, t->id, f, vesc);
            free(vesc);
            t->state = ST_PENDING;
            return true;
        }

        case MUT_INSERT_BEFORE: {
            /* Don't allow id collisions */
            if (wf_find(wf, m->insert_id)) {
                log_eventv(c, "mutation_rejected",
                    "\"id\":\"%s\",\"verb\":\"INSERT_BEFORE\",\"task\":\"%s\",\"reason\":\"id_exists\"",
                    mid, t->id);
                return false;
            }
            Task *nt = xcalloc(1, sizeof *nt);
            nt->id = xstrdup(m->insert_id);
            nt->type = TASK_SHELL;
            nt->cmd = xstrdup(m->insert_cmd);
            nt->state = ST_PENDING;
            nt->origin.source = xstrdup("planner");
            nt->origin.parent_mutation = xstrdup(mid);
            nt->origin.parent_task = xstrdup(t->id);
            /* inherit deps from the failing task so it runs at the right point */
            for (size_t i = 0; i < t->deps.n; i++)
                sl_push(&nt->deps, xstrdup(t->deps.items[i]));
            wf_add(wf, nt);
            /* failing task now depends on the new task */
            sl_push(&t->deps, xstrdup(nt->id));
            char *cesc = json_escape(m->insert_cmd);
            log_eventv(c, "mutation_applied",
                "\"id\":\"%s\",\"verb\":\"INSERT_BEFORE\",\"task\":\"%s\",\"new_task\":\"%s\",\"cmd\":\"%s\"",
                mid, t->id, m->insert_id, cesc);
            free(cesc);
            t->state = ST_PENDING;
            return true;
        }
    }
    return false;
}

/* ============================================================
 *  Scheduler: ready-queue + worker thread per ready task per pass
 *
 * After mutations the graph may grow, so we loop until no progress.
 * ============================================================ */

static bool deps_satisfied(Workflow *wf, Task *t) {
    for (size_t i = 0; i < t->deps.n; i++) {
        Task *d = wf_find(wf, t->deps.items[i]);
        if (!d) return false;
        if (d->state != ST_DONE && d->state != ST_GATED_OUT) return false;
    }
    return true;
}

static bool any_dep_failed(Workflow *wf, Task *t) {
    for (size_t i = 0; i < t->deps.n; i++) {
        Task *d = wf_find(wf, t->deps.items[i]);
        if (d && (d->state == ST_FAILED || d->state == ST_SKIPPED)) return true;
    }
    return false;
}

/* `when` gating: if t has when="<decision_id>", it only runs if that
 * decision succeeded AND chose this task's id. */
static bool when_satisfied(Workflow *wf, Task *t) {
    if (!t->when) return true;
    Task *d = wf_find(wf, t->when);
    if (!d || d->state != ST_DONE) return false;
    if (!d->chosen) return false;
    return strcmp(d->chosen, t->id) == 0;
}

typedef struct {
    Workflow *wf;
    RunCtx   *c;
    Task     *task;
} ThreadArg;

static int worker_thread(void *arg) {
    ThreadArg *ta = (ThreadArg *)arg;
    Task *t = ta->task;
    RunCtx *c = ta->c;
    Workflow *wf = ta->wf;

    t->state = ST_RUNNING;
    log_eventv(c, "task_started",
        "\"task\":\"%s\",\"task_type\":%d,\"origin\":\"%s\"",
        t->id, (int)t->type, t->origin.source ? t->origin.source : "yaml");

    int attempts = t->retries + 1;
    int rc = 1;
    for (int i = 0; i < attempts; i++) {
        rc = exec_task_once(t, c);
        if (rc == 0) break;
        log_eventv(c, "task_retry",
            "\"task\":\"%s\",\"attempt\":%d,\"rc\":%d", t->id, i + 1, rc);
    }
    t->exit_code = rc;

    /* write status */
    char outp[1024], errp[1024], stp[1024];
    run_paths(c, t->id, outp, errp, stp, sizeof outp);
    FILE *sf = fopen(stp, "wb"); if (sf) { fprintf(sf, "%d\n", rc); fclose(sf); }

    if (rc == 0) {
        t->state = ST_DONE;
        if (t->type == TASK_DECISION && t->chosen) {
            log_eventv(c, "decision_made",
                "\"task\":\"%s\",\"chosen\":\"%s\"", t->id, t->chosen);
        }
        log_eventv(c, "task_succeeded", "\"task\":\"%s\"", t->id);
        return 0;
    }

    /* failure: try planner if budget remaining */
    if (t->on_failure.model && t->on_failure.used < t->on_failure.budget) {
        t->on_failure.used++;
        log_eventv(c, "planner_invoked",
            "\"task\":\"%s\",\"attempt\":%d,\"budget\":%d",
            t->id, t->on_failure.used, t->on_failure.budget);
        Mutation m = invoke_planner(t, c);
        bool requeue = apply_mutation(wf, t, &m, c);
        free_mutation(&m);
        if (requeue) {
            /* state already reset to ST_PENDING by apply_mutation */
            return 0;
        }
    }

    t->state = ST_FAILED;
    log_eventv(c, "task_failed",
        "\"task\":\"%s\",\"rc\":%d", t->id, rc);
    return rc;
}

static int run_workflow(Workflow *wf, RunCtx *c) {
    log_eventv(c, "run_started",
        "\"workflow\":\"%s\",\"tasks\":%zu", wf->name, wf->n_tasks);

    while (true) {
        /* mark tasks whose deps failed as skipped */
        for (size_t i = 0; i < wf->n_tasks; i++) {
            Task *t = wf->tasks[i];
            if (t->state == ST_PENDING && any_dep_failed(wf, t)) {
                t->state = ST_SKIPPED;
                log_eventv(c, "task_skipped",
                    "\"task\":\"%s\",\"reason\":\"dep_failed\"", t->id);
            }
        }
        /* mark tasks whose `when` cannot be satisfied as gated-out (only after
         * the gating decision is in a terminal state) */
        for (size_t i = 0; i < wf->n_tasks; i++) {
            Task *t = wf->tasks[i];
            if (t->state == ST_PENDING && t->when) {
                Task *d = wf_find(wf, t->when);
                if (d && (d->state == ST_DONE || d->state == ST_FAILED ||
                          d->state == ST_SKIPPED || d->state == ST_GATED_OUT)) {
                    if (!when_satisfied(wf, t)) {
                        t->state = ST_GATED_OUT;
                        log_eventv(c, "task_gated_out",
                            "\"task\":\"%s\",\"gate\":\"%s\"",
                            t->id, t->when);
                    }
                }
            }
        }

        /* gather ready tasks */
        size_t ready_cnt = 0;
        for (size_t i = 0; i < wf->n_tasks; i++) {
            Task *t = wf->tasks[i];
            if (t->state == ST_PENDING && deps_satisfied(wf, t) && when_satisfied(wf, t)) {
                ready_cnt++;
            }
        }
        if (ready_cnt == 0) break;

        thrd_t   *threads = xcalloc(ready_cnt, sizeof(thrd_t));
        ThreadArg *args   = xcalloc(ready_cnt, sizeof(ThreadArg));
        size_t k = 0;
        size_t snapshot = wf->n_tasks;  /* mutations may grow wf during this pass */
        for (size_t i = 0; i < snapshot; i++) {
            Task *t = wf->tasks[i];
            if (t->state == ST_PENDING && deps_satisfied(wf, t) && when_satisfied(wf, t)) {
                args[k] = (ThreadArg){ .wf = wf, .c = c, .task = t };
                if (thrd_create(&threads[k], worker_thread, &args[k]) != thrd_success)
                    die("thrd_create failed");
                k++;
                if (k == ready_cnt) break;
            }
        }
        for (size_t i = 0; i < k; i++) thrd_join(threads[i], NULL);
        free(threads); free(args);
    }

    /* summary */
    size_t ok = 0, fail = 0, skip = 0, gated = 0;
    for (size_t i = 0; i < wf->n_tasks; i++) {
        switch (wf->tasks[i]->state) {
            case ST_DONE: ok++; break;
            case ST_FAILED: fail++; break;
            case ST_SKIPPED: skip++; break;
            case ST_GATED_OUT: gated++; break;
            default: break;
        }
    }
    log_eventv(c, "run_finished",
        "\"succeeded\":%zu,\"failed\":%zu,\"skipped\":%zu,\"gated_out\":%zu",
        ok, fail, skip, gated);
    fprintf(stdout,
        "\n=== summary ===\n"
        "succeeded:  %zu\nfailed:     %zu\nskipped:    %zu\ngated_out:  %zu\nlogs:       %s\n",
        ok, fail, skip, gated, c->run_dir);
    return fail == 0 ? 0 : 1;
}

/* ============================================================
 *  Replay: read events.jsonl, print human-readable timeline.
 *  Pure read-only audit tool; no LLM calls.
 * ============================================================ */

static int cmd_replay(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "cannot open %s\n", path); return 1; }
    char line[4096];
    while (fgets(line, sizeof line, f)) {
        char *ts = json_str_field(line, "ts");
        char *type = json_str_field(line, "type");
        char *task = json_str_field(line, "task");
        char *verb = json_str_field(line, "verb");
        char *chosen = json_str_field(line, "chosen");
        printf("%s  %-20s", ts ? ts : "?", type ? type : "?");
        if (task)   printf("  task=%s", task);
        if (verb)   printf("  verb=%s", verb);
        if (chosen) printf("  chosen=%s", chosen);
        printf("\n");
        free(ts); free(type); free(task); free(verb); free(chosen);
    }
    fclose(f);
    return 0;
}

/* ============================================================
 *  CLI
 * ============================================================ */

static void usage(void) {
    fprintf(stderr,
      "liteflow - dynamic DAG runner with LLM-in-the-loop control\n\n"
      "Usage:\n"
      "  liteflow run <workflow.yaml> [--logdir DIR]\n"
      "  liteflow validate <workflow.yaml>\n"
      "  liteflow replay <events.jsonl>\n"
      "  liteflow help\n\n"
      "Environment:\n"
      "  OPENAI_API_KEY     required for llm/decision/on_failure\n"
      "  OPENAI_BASE_URL    optional; defaults to https://api.openai.com/v1\n");
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 1; }
    const char *cmd = argv[1];

    if (!strcmp(cmd, "help") || !strcmp(cmd, "-h") || !strcmp(cmd, "--help")) {
        usage(); return 0;
    }
    if (!strcmp(cmd, "replay")) {
        if (argc < 3) { usage(); return 1; }
        return cmd_replay(argv[2]);
    }
    if (!strcmp(cmd, "validate")) {
        if (argc < 3) { usage(); return 1; }
        Workflow *wf = parse_workflow(argv[2]);
        printf("OK: '%s' with %zu task(s)\n", wf->name, wf->n_tasks);
        for (size_t i = 0; i < wf->n_tasks; i++) {
            Task *t = wf->tasks[i];
            printf("  - %s (type=%d, deps=%zu, retries=%d, on_failure=%s, budget=%d)\n",
                t->id, (int)t->type, t->deps.n, t->retries,
                t->on_failure.model ? t->on_failure.model : "(none)",
                t->on_failure.budget);
            if (t->type == TASK_DECISION) {
                printf("      branches:");
                for (size_t b = 0; b < t->branches.n; b++) printf(" %s", t->branches.items[b]);
                printf("\n");
            }
            if (t->when) printf("      when: %s\n", t->when);
        }
        free_workflow(wf);
        return 0;
    }
    if (!strcmp(cmd, "run")) {
        if (argc < 3) { usage(); return 1; }
        const char *yaml = argv[2];
        const char *logdir = "logs";
        for (int i = 3; i < argc; i++) {
            if (!strcmp(argv[i], "--logdir") && i + 1 < argc) logdir = argv[++i];
        }
        Workflow *wf = parse_workflow(yaml);
        RunCtx *c = make_run_ctx(logdir, wf->name);
        printf("liteflow: running '%s' (%zu tasks)  logs: %s\n",
               wf->name, wf->n_tasks, c->run_dir);
        int rc = run_workflow(wf, c);
        free_run_ctx(c);
        free_workflow(wf);
        return rc;
    }
    usage();
    return 1;
}
