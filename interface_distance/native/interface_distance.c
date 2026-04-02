#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

typedef struct {
    char *partner_domain;
    char *row_key;
    uint32_t *columns;
    size_t column_count;
} InterfaceEntry;

typedef struct {
    InterfaceEntry *items;
    size_t count;
    size_t capacity;
} EntryArray;

typedef struct {
    const char *input_file;
    const char *output_file;
    const char *metadata_out;
} Args;

typedef struct {
    const char *path;
    const char *cur;
    const char *end;
    char error[256];
} Parser;

typedef struct {
    size_t start;
    size_t end;
    size_t byte_count;
    uint8_t *buffer;
    bool ready;
} RowBatch;

typedef struct {
    const EntryArray *entries;
    RowBatch *batches;
    size_t batch_count;
    atomic_size_t next_batch;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int error;
    char error_message[256];
} WorkerContext;

static void free_entry(InterfaceEntry *entry) {
    if (entry == NULL) {
        return;
    }
    free(entry->partner_domain);
    free(entry->row_key);
    free(entry->columns);
    entry->partner_domain = NULL;
    entry->row_key = NULL;
    entry->columns = NULL;
    entry->column_count = 0;
}

static void free_entries(EntryArray *entries) {
    if (entries == NULL) {
        return;
    }
    for (size_t index = 0; index < entries->count; index += 1) {
        free_entry(&entries->items[index]);
    }
    free(entries->items);
    entries->items = NULL;
    entries->count = 0;
    entries->capacity = 0;
}

static void usage(FILE *stream) {
    fprintf(
        stream,
        "Usage: interface_distance --input-file PATH --output-file PATH [--metadata-out PATH]\n"
    );
}

static bool is_path_separator(char value) {
    return value == '/' || value == '\\';
}

static char *portable_strdup(const char *value) {
    size_t length = strlen(value) + 1;
    char *copy = malloc(length);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, value, length);
    return copy;
}

static int create_directory(const char *path) {
#ifdef _WIN32
    return _mkdir(path);
#else
    return mkdir(path, 0777);
#endif
}

static int parse_args(int argc, char **argv, Args *args) {
    memset(args, 0, sizeof(*args));
    for (int index = 1; index < argc; index += 1) {
        const char *arg = argv[index];
        if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
            usage(stdout);
            exit(0);
        }
        if (index + 1 >= argc) {
            fprintf(stderr, "missing value for %s\n", arg);
            usage(stderr);
            return 1;
        }
        const char *value = argv[index + 1];
        if (strcmp(arg, "--input-file") == 0) {
            args->input_file = value;
        } else if (strcmp(arg, "--output-file") == 0) {
            args->output_file = value;
        } else if (strcmp(arg, "--metadata-out") == 0) {
            args->metadata_out = value;
        } else {
            fprintf(stderr, "unknown argument: %s\n", arg);
            usage(stderr);
            return 1;
        }
        index += 1;
    }
    if (args->input_file == NULL || args->output_file == NULL) {
        usage(stderr);
        return 1;
    }
    return 0;
}

static void parser_error(Parser *parser, const char *format, ...) {
    if (parser->error[0] != '\0') {
        return;
    }
    va_list args;
    va_start(args, format);
    vsnprintf(parser->error, sizeof(parser->error), format, args);
    va_end(args);
}

static void skip_ws(Parser *parser) {
    while (parser->cur < parser->end && isspace((unsigned char) *parser->cur)) {
        parser->cur += 1;
    }
}

static bool parser_consume(Parser *parser, char expected) {
    skip_ws(parser);
    if (parser->cur >= parser->end || *parser->cur != expected) {
        parser_error(parser, "expected '%c' while parsing %s", expected, parser->path);
        return false;
    }
    parser->cur += 1;
    return true;
}

static int hex_value(char value) {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return value - 'A' + 10;
    }
    return -1;
}

static bool skip_string(Parser *parser) {
    if (!parser_consume(parser, '"')) {
        return false;
    }
    while (parser->cur < parser->end) {
        char current = *parser->cur++;
        if (current == '"') {
            return true;
        }
        if (current == '\\') {
            if (parser->cur >= parser->end) {
                parser_error(parser, "unterminated escape sequence in %s", parser->path);
                return false;
            }
            if (*parser->cur == 'u') {
                parser->cur += 1;
                for (int index = 0; index < 4; index += 1) {
                    if (parser->cur >= parser->end || hex_value(*parser->cur) < 0) {
                        parser_error(parser, "invalid unicode escape in %s", parser->path);
                        return false;
                    }
                    parser->cur += 1;
                }
            } else {
                parser->cur += 1;
            }
        }
    }
    parser_error(parser, "unterminated string in %s", parser->path);
    return false;
}

static bool parse_string(Parser *parser, char **out_value) {
    skip_ws(parser);
    if (parser->cur >= parser->end || *parser->cur != '"') {
        parser_error(parser, "expected string while parsing %s", parser->path);
        return false;
    }

    parser->cur += 1;
    size_t capacity = 32;
    size_t length = 0;
    char *buffer = malloc(capacity);
    if (buffer == NULL) {
        parser_error(parser, "out of memory while parsing %s", parser->path);
        return false;
    }

    while (parser->cur < parser->end) {
        char current = *parser->cur++;
        if (current == '"') {
            buffer[length] = '\0';
            *out_value = buffer;
            return true;
        }
        if (current == '\\') {
            if (parser->cur >= parser->end) {
                free(buffer);
                parser_error(parser, "unterminated escape sequence in %s", parser->path);
                return false;
            }
            char escaped = *parser->cur++;
            if (escaped == 'u') {
                int codepoint = 0;
                for (int index = 0; index < 4; index += 1) {
                    if (parser->cur >= parser->end) {
                        free(buffer);
                        parser_error(parser, "invalid unicode escape in %s", parser->path);
                        return false;
                    }
                    int value = hex_value(*parser->cur++);
                    if (value < 0) {
                        free(buffer);
                        parser_error(parser, "invalid unicode escape in %s", parser->path);
                        return false;
                    }
                    codepoint = (codepoint << 4) | value;
                }
                current = (codepoint >= 0 && codepoint <= 0x7f) ? (char) codepoint : '?';
            } else {
                switch (escaped) {
                    case '"': current = '"'; break;
                    case '\\': current = '\\'; break;
                    case '/': current = '/'; break;
                    case 'b': current = '\b'; break;
                    case 'f': current = '\f'; break;
                    case 'n': current = '\n'; break;
                    case 'r': current = '\r'; break;
                    case 't': current = '\t'; break;
                    default:
                        free(buffer);
                        parser_error(parser, "unsupported escape in %s", parser->path);
                        return false;
                }
            }
        }
        if (length + 2 > capacity) {
            capacity *= 2;
            char *resized = realloc(buffer, capacity);
            if (resized == NULL) {
                free(buffer);
                parser_error(parser, "out of memory while parsing %s", parser->path);
                return false;
            }
            buffer = resized;
        }
        buffer[length++] = current;
    }

    free(buffer);
    parser_error(parser, "unterminated string in %s", parser->path);
    return false;
}

static bool skip_number(Parser *parser) {
    skip_ws(parser);
    const char *start = parser->cur;
    if (parser->cur < parser->end && (*parser->cur == '-' || *parser->cur == '+')) {
        parser->cur += 1;
    }
    while (parser->cur < parser->end && isdigit((unsigned char) *parser->cur)) {
        parser->cur += 1;
    }
    if (parser->cur < parser->end && *parser->cur == '.') {
        parser->cur += 1;
        while (parser->cur < parser->end && isdigit((unsigned char) *parser->cur)) {
            parser->cur += 1;
        }
    }
    if (parser->cur < parser->end && (*parser->cur == 'e' || *parser->cur == 'E')) {
        parser->cur += 1;
        if (parser->cur < parser->end && (*parser->cur == '-' || *parser->cur == '+')) {
            parser->cur += 1;
        }
        while (parser->cur < parser->end && isdigit((unsigned char) *parser->cur)) {
            parser->cur += 1;
        }
    }
    if (parser->cur == start) {
        parser_error(parser, "expected number while parsing %s", parser->path);
        return false;
    }
    return true;
}

static bool skip_literal(Parser *parser, const char *literal) {
    skip_ws(parser);
    size_t literal_length = strlen(literal);
    if ((size_t) (parser->end - parser->cur) < literal_length ||
        strncmp(parser->cur, literal, literal_length) != 0) {
        parser_error(parser, "expected literal '%s' while parsing %s", literal, parser->path);
        return false;
    }
    parser->cur += literal_length;
    return true;
}

static bool skip_value(Parser *parser);

static bool skip_array(Parser *parser) {
    if (!parser_consume(parser, '[')) {
        return false;
    }
    skip_ws(parser);
    if (parser->cur < parser->end && *parser->cur == ']') {
        parser->cur += 1;
        return true;
    }
    while (true) {
        if (!skip_value(parser)) {
            return false;
        }
        skip_ws(parser);
        if (parser->cur >= parser->end) {
            parser_error(parser, "unterminated array in %s", parser->path);
            return false;
        }
        if (*parser->cur == ']') {
            parser->cur += 1;
            return true;
        }
        if (*parser->cur != ',') {
            parser_error(parser, "expected ',' in array while parsing %s", parser->path);
            return false;
        }
        parser->cur += 1;
    }
}

static bool skip_object(Parser *parser) {
    if (!parser_consume(parser, '{')) {
        return false;
    }
    skip_ws(parser);
    if (parser->cur < parser->end && *parser->cur == '}') {
        parser->cur += 1;
        return true;
    }
    while (true) {
        if (!skip_string(parser)) {
            return false;
        }
        if (!parser_consume(parser, ':')) {
            return false;
        }
        if (!skip_value(parser)) {
            return false;
        }
        skip_ws(parser);
        if (parser->cur >= parser->end) {
            parser_error(parser, "unterminated object in %s", parser->path);
            return false;
        }
        if (*parser->cur == '}') {
            parser->cur += 1;
            return true;
        }
        if (*parser->cur != ',') {
            parser_error(parser, "expected ',' in object while parsing %s", parser->path);
            return false;
        }
        parser->cur += 1;
    }
}

static bool skip_value(Parser *parser) {
    skip_ws(parser);
    if (parser->cur >= parser->end) {
        parser_error(parser, "unexpected end of input while parsing %s", parser->path);
        return false;
    }
    switch (*parser->cur) {
        case '"':
            return skip_string(parser);
        case '{':
            return skip_object(parser);
        case '[':
            return skip_array(parser);
        case 't':
            return skip_literal(parser, "true");
        case 'f':
            return skip_literal(parser, "false");
        case 'n':
            return skip_literal(parser, "null");
        default:
            return skip_number(parser);
    }
}

static bool parse_uint32_value(Parser *parser, uint32_t *out_value) {
    skip_ws(parser);
    errno = 0;
    char *end_ptr = NULL;
    unsigned long parsed = strtoul(parser->cur, &end_ptr, 10);
    if (parser->cur == end_ptr || errno != 0 || parsed > UINT32_MAX) {
        parser_error(parser, "invalid integer while parsing %s", parser->path);
        return false;
    }
    parser->cur = end_ptr;
    *out_value = (uint32_t) parsed;
    return true;
}

static int compare_u32(const void *left, const void *right) {
    const uint32_t a = *(const uint32_t *) left;
    const uint32_t b = *(const uint32_t *) right;
    if (a < b) {
        return -1;
    }
    if (a > b) {
        return 1;
    }
    return 0;
}

static bool parse_columns_array(Parser *parser, uint32_t **out_columns, size_t *out_count) {
    if (!parser_consume(parser, '[')) {
        return false;
    }
    size_t capacity = 16;
    size_t count = 0;
    uint32_t *columns = malloc(capacity * sizeof(*columns));
    if (columns == NULL) {
        parser_error(parser, "out of memory while parsing %s", parser->path);
        return false;
    }

    skip_ws(parser);
    if (parser->cur < parser->end && *parser->cur == ']') {
        parser->cur += 1;
        *out_columns = columns;
        *out_count = 0;
        return true;
    }

    while (true) {
        uint32_t value = 0;
        if (!parse_uint32_value(parser, &value)) {
            free(columns);
            return false;
        }
        if (count == capacity) {
            capacity *= 2;
            uint32_t *resized = realloc(columns, capacity * sizeof(*columns));
            if (resized == NULL) {
                free(columns);
                parser_error(parser, "out of memory while parsing %s", parser->path);
                return false;
            }
            columns = resized;
        }
        columns[count++] = value;

        skip_ws(parser);
        if (parser->cur >= parser->end) {
            free(columns);
            parser_error(parser, "unterminated array while parsing %s", parser->path);
            return false;
        }
        if (*parser->cur == ']') {
            parser->cur += 1;
            break;
        }
        if (*parser->cur != ',') {
            free(columns);
            parser_error(parser, "expected ',' in array while parsing %s", parser->path);
            return false;
        }
        parser->cur += 1;
    }

    qsort(columns, count, sizeof(*columns), compare_u32);
    size_t dedup_count = 0;
    for (size_t index = 0; index < count; index += 1) {
        if (dedup_count == 0 || columns[index] != columns[dedup_count - 1]) {
            columns[dedup_count++] = columns[index];
        }
    }

    *out_columns = columns;
    *out_count = dedup_count;
    return true;
}

static bool append_entry(
    EntryArray *entries,
    char *partner_domain,
    char *row_key,
    uint32_t *columns,
    size_t column_count
) {
    if (entries->count == entries->capacity) {
        size_t new_capacity = entries->capacity == 0 ? 64 : entries->capacity * 2;
        InterfaceEntry *resized = realloc(entries->items, new_capacity * sizeof(*resized));
        if (resized == NULL) {
            return false;
        }
        entries->items = resized;
        entries->capacity = new_capacity;
    }

    entries->items[entries->count].partner_domain = partner_domain;
    entries->items[entries->count].row_key = row_key;
    entries->items[entries->count].columns = columns;
    entries->items[entries->count].column_count = column_count;
    entries->count += 1;
    return true;
}

static bool parse_payload_object(
    Parser *parser,
    uint32_t **out_columns,
    size_t *out_count
) {
    *out_columns = NULL;
    *out_count = 0;

    if (!parser_consume(parser, '{')) {
        return false;
    }

    skip_ws(parser);
    if (parser->cur < parser->end && *parser->cur == '}') {
        parser->cur += 1;
        return true;
    }

    while (true) {
        char *key = NULL;
        if (!parse_string(parser, &key)) {
            return false;
        }
        if (!parser_consume(parser, ':')) {
            free(key);
            return false;
        }

        if (strcmp(key, "interface_msa_columns_a") == 0) {
            free(key);
            free(*out_columns);
            *out_columns = NULL;
            *out_count = 0;
            if (!parse_columns_array(parser, out_columns, out_count)) {
                return false;
            }
        } else {
            free(key);
            if (!skip_value(parser)) {
                return false;
            }
        }

        skip_ws(parser);
        if (parser->cur >= parser->end) {
            parser_error(parser, "unterminated payload object in %s", parser->path);
            return false;
        }
        if (*parser->cur == '}') {
            parser->cur += 1;
            return true;
        }
        if (*parser->cur != ',') {
            parser_error(parser, "expected ',' in payload object while parsing %s", parser->path);
            return false;
        }
        parser->cur += 1;
    }
}

static int compare_entries(const void *left, const void *right) {
    const InterfaceEntry *a = (const InterfaceEntry *) left;
    const InterfaceEntry *b = (const InterfaceEntry *) right;
    int partner_cmp = strcmp(a->partner_domain, b->partner_domain);
    if (partner_cmp != 0) {
        return partner_cmp;
    }
    return strcmp(a->row_key, b->row_key);
}

static bool parse_entries(Parser *parser, EntryArray *entries) {
    if (!parser_consume(parser, '{')) {
        return false;
    }
    skip_ws(parser);
    if (parser->cur < parser->end && *parser->cur == '}') {
        parser->cur += 1;
        return true;
    }

    while (true) {
        char *partner_domain = NULL;
        if (!parse_string(parser, &partner_domain)) {
            return false;
        }
        if (!parser_consume(parser, ':')) {
            free(partner_domain);
            return false;
        }
        if (!parser_consume(parser, '{')) {
            free(partner_domain);
            return false;
        }

        skip_ws(parser);
        if (!(parser->cur < parser->end && *parser->cur == '}')) {
            while (true) {
                char *row_key = NULL;
                uint32_t *columns = NULL;
                size_t column_count = 0;
                if (!parse_string(parser, &row_key)) {
                    free(partner_domain);
                    return false;
                }
                if (!parser_consume(parser, ':')) {
                    free(row_key);
                    free(partner_domain);
                    return false;
                }
                if (!parse_payload_object(parser, &columns, &column_count)) {
                    free(row_key);
                    free(columns);
                    free(partner_domain);
                    return false;
                }
                if (column_count > 0) {
                    char *partner_copy = portable_strdup(partner_domain);
                    if (partner_copy == NULL ||
                        !append_entry(entries, partner_copy, row_key, columns, column_count)) {
                        free(partner_copy);
                        free(row_key);
                        free(columns);
                        free(partner_domain);
                        parser_error(parser, "out of memory while parsing %s", parser->path);
                        return false;
                    }
                } else {
                    free(row_key);
                    free(columns);
                }

                skip_ws(parser);
                if (parser->cur >= parser->end) {
                    free(partner_domain);
                    parser_error(parser, "unterminated partner object in %s", parser->path);
                    return false;
                }
                if (*parser->cur == '}') {
                    break;
                }
                if (*parser->cur != ',') {
                    free(partner_domain);
                    parser_error(parser, "expected ',' in partner object while parsing %s", parser->path);
                    return false;
                }
                parser->cur += 1;
            }
        }
        parser->cur += 1;
        free(partner_domain);

        skip_ws(parser);
        if (parser->cur >= parser->end) {
            parser_error(parser, "unterminated top-level object in %s", parser->path);
            return false;
        }
        if (*parser->cur == '}') {
            parser->cur += 1;
            break;
        }
        if (*parser->cur != ',') {
            parser_error(parser, "expected ',' in top-level object while parsing %s", parser->path);
            return false;
        }
        parser->cur += 1;
    }

    qsort(entries->items, entries->count, sizeof(entries->items[0]), compare_entries);
    return true;
}

static char *read_text_file(const char *path, size_t *out_length) {
    FILE *handle = fopen(path, "rb");
    if (handle == NULL) {
        return NULL;
    }
    if (fseek(handle, 0, SEEK_END) != 0) {
        fclose(handle);
        return NULL;
    }
    long file_size = ftell(handle);
    if (file_size < 0) {
        fclose(handle);
        return NULL;
    }
    if (fseek(handle, 0, SEEK_SET) != 0) {
        fclose(handle);
        return NULL;
    }
    char *buffer = malloc((size_t) file_size + 1);
    if (buffer == NULL) {
        fclose(handle);
        return NULL;
    }
    size_t read_length = fread(buffer, 1, (size_t) file_size, handle);
    fclose(handle);
    if (read_length != (size_t) file_size) {
        free(buffer);
        return NULL;
    }
    buffer[read_length] = '\0';
    if (out_length != NULL) {
        *out_length = read_length;
    }
    return buffer;
}

static bool ensure_parent_dirs(const char *path) {
    char *copy = portable_strdup(path);
    if (copy == NULL) {
        return false;
    }
    char *cursor = copy;
#ifdef _WIN32
    if (isalpha((unsigned char) copy[0]) && copy[1] == ':' && is_path_separator(copy[2])) {
        cursor = copy + 3;
    } else
#endif
    if (is_path_separator(copy[0])) {
        cursor = copy + 1;
    }
    for (; *cursor != '\0'; cursor += 1) {
        if (!is_path_separator(*cursor)) {
            continue;
        }
        char separator = *cursor;
        *cursor = '\0';
        if (*copy != '\0' && create_directory(copy) != 0 && errno != EEXIST) {
            free(copy);
            return false;
        }
        *cursor = separator;
    }
    free(copy);
    return true;
}

static bool write_metadata_file(const EntryArray *entries, const char *metadata_out) {
    if (!ensure_parent_dirs(metadata_out)) {
        fprintf(stderr, "failed to create metadata parent directories for %s: %s\n", metadata_out, strerror(errno));
        return false;
    }
    FILE *handle = fopen(metadata_out, "wb");
    if (handle == NULL) {
        fprintf(stderr, "failed to create %s: %s\n", metadata_out, strerror(errno));
        return false;
    }
    if (fprintf(handle, "index\tpartner_domain\trow_key\tcolumn_count\n") < 0) {
        fprintf(stderr, "failed to write metadata header to %s\n", metadata_out);
        fclose(handle);
        return false;
    }
    for (size_t index = 0; index < entries->count; index += 1) {
        const InterfaceEntry *entry = &entries->items[index];
        if (fprintf(
                handle,
                "%zu\t%s\t%s\t%zu\n",
                index,
                entry->partner_domain,
                entry->row_key,
                entry->column_count
            ) < 0) {
            fprintf(stderr, "failed to write metadata row to %s\n", metadata_out);
            fclose(handle);
            return false;
        }
    }
    if (fclose(handle) != 0) {
        fprintf(stderr, "failed to flush %s: %s\n", metadata_out, strerror(errno));
        return false;
    }
    return true;
}

static inline double overlap_distance(const uint32_t *left, size_t left_count, const uint32_t *right, size_t right_count) {
    if (left_count == 0 && right_count == 0) {
        return 0.0;
    }
    if (left_count == 0 || right_count == 0) {
        return 1.0;
    }

    size_t left_index = 0;
    size_t right_index = 0;
    size_t intersection = 0;
    while (left_index < left_count && right_index < right_count) {
        if (left[left_index] < right[right_index]) {
            left_index += 1;
        } else if (left[left_index] > right[right_index]) {
            right_index += 1;
        } else {
            intersection += 1;
            left_index += 1;
            right_index += 1;
        }
    }
    size_t minimum_size = left_count < right_count ? left_count : right_count;
    return 1.0 - ((double) intersection / (double) minimum_size);
}

static inline uint16_t quantize_unit_interval(double value) {
    double clamped = value;
    if (clamped < 0.0) {
        clamped = 0.0;
    } else if (clamped > 1.0) {
        clamped = 1.0;
    }
    return (uint16_t) llround(clamped * (double) UINT16_MAX);
}

static size_t condensed_size(size_t entry_count) {
    return (entry_count * (entry_count - 1)) / 2;
}

static size_t available_worker_count(void) {
#ifdef _WIN32
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    if (info.dwNumberOfProcessors == 0) {
        return 1;
    }
    return (size_t) info.dwNumberOfProcessors;
#else
    long result = sysconf(_SC_NPROCESSORS_ONLN);
    if (result <= 0) {
        return 1;
    }
    return (size_t) result;
#endif
}

static size_t row_output_bytes(size_t entry_count, size_t left_index) {
    return (entry_count - left_index - 1) * sizeof(uint16_t);
}

static size_t clamp_size(size_t value, size_t minimum, size_t maximum) {
    if (value < minimum) {
        return minimum;
    }
    if (value > maximum) {
        return maximum;
    }
    return value;
}

static size_t batch_target_bytes(size_t entry_count, size_t worker_count) {
    size_t total_bytes = condensed_size(entry_count) * sizeof(uint16_t);
    size_t denominator = worker_count * 8;
    if (denominator == 0) {
        denominator = 1;
    }
    size_t target = total_bytes / denominator;
    return clamp_size(target, 256 * 1024, 4 * 1024 * 1024);
}

static RowBatch *build_row_batches(size_t entry_count, size_t worker_count, size_t *out_batch_count) {
    *out_batch_count = 0;
    size_t row_count = entry_count > 0 ? entry_count - 1 : 0;
    if (row_count == 0) {
        return NULL;
    }

    size_t target_bytes = batch_target_bytes(entry_count, worker_count);
    size_t capacity = 64;
    size_t count = 0;
    RowBatch *batches = malloc(capacity * sizeof(*batches));
    if (batches == NULL) {
        return NULL;
    }

    size_t batch_start = 0;
    while (batch_start < row_count) {
        size_t batch_end = batch_start;
        size_t byte_count = 0;
        while (batch_end < row_count) {
            size_t row_bytes = row_output_bytes(entry_count, batch_end);
            if (batch_end > batch_start && byte_count + row_bytes > target_bytes) {
                break;
            }
            byte_count += row_bytes;
            batch_end += 1;
        }
        if (count == capacity) {
            capacity *= 2;
            RowBatch *resized = realloc(batches, capacity * sizeof(*batches));
            if (resized == NULL) {
                free(batches);
                return NULL;
            }
            batches = resized;
        }
        batches[count].start = batch_start;
        batches[count].end = batch_end;
        batches[count].byte_count = byte_count;
        batches[count].buffer = NULL;
        batches[count].ready = false;
        count += 1;
        batch_start = batch_end;
    }

    *out_batch_count = count;
    return batches;
}

static uint8_t *build_batch_buffer(const EntryArray *entries, const RowBatch *batch) {
    uint8_t *buffer = malloc(batch->byte_count == 0 ? 1 : batch->byte_count);
    if (buffer == NULL) {
        return NULL;
    }
    uint8_t *cursor = buffer;
    for (size_t left_index = batch->start; left_index < batch->end; left_index += 1) {
        const InterfaceEntry *left = &entries->items[left_index];
        for (size_t right_index = left_index + 1; right_index < entries->count; right_index += 1) {
            const InterfaceEntry *right = &entries->items[right_index];
            uint16_t quantized = quantize_unit_interval(
                overlap_distance(
                    left->columns,
                    left->column_count,
                    right->columns,
                    right->column_count
                )
            );
            cursor[0] = (uint8_t) (quantized & 0xffu);
            cursor[1] = (uint8_t) ((quantized >> 8) & 0xffu);
            cursor += 2;
        }
    }
    return buffer;
}

static void set_worker_error(WorkerContext *context, const char *format, ...) {
    pthread_mutex_lock(&context->mutex);
    if (!context->error) {
        context->error = 1;
        va_list args;
        va_start(args, format);
        vsnprintf(context->error_message, sizeof(context->error_message), format, args);
        va_end(args);
    }
    pthread_cond_broadcast(&context->cond);
    pthread_mutex_unlock(&context->mutex);
}

static void *worker_main(void *opaque) {
    WorkerContext *context = (WorkerContext *) opaque;
    while (true) {
        size_t batch_index = atomic_fetch_add(&context->next_batch, 1);
        if (batch_index >= context->batch_count) {
            return NULL;
        }
        RowBatch *batch = &context->batches[batch_index];
        uint8_t *buffer = build_batch_buffer(context->entries, batch);
        if (buffer == NULL) {
            set_worker_error(context, "out of memory while computing batch %zu", batch_index);
            return NULL;
        }
        pthread_mutex_lock(&context->mutex);
        batch->buffer = buffer;
        batch->ready = true;
        pthread_cond_broadcast(&context->cond);
        pthread_mutex_unlock(&context->mutex);
    }
}

static bool write_batches_sequential(const EntryArray *entries, RowBatch *batches, size_t batch_count, FILE *handle) {
    (void) batches;
    for (size_t batch_index = 0; batch_index < batch_count; batch_index += 1) {
        uint8_t *buffer = build_batch_buffer(entries, &batches[batch_index]);
        if (buffer == NULL) {
            fprintf(stderr, "out of memory while computing batch %zu\n", batch_index);
            return false;
        }
        if (fwrite(buffer, 1, batches[batch_index].byte_count, handle) != batches[batch_index].byte_count) {
            fprintf(stderr, "failed to write output batch %zu\n", batch_index);
            free(buffer);
            return false;
        }
        free(buffer);
    }
    return true;
}

static bool write_batches_parallel(const EntryArray *entries, RowBatch *batches, size_t batch_count, size_t worker_count, FILE *handle) {
    WorkerContext context;
    context.entries = entries;
    context.batches = batches;
    context.batch_count = batch_count;
    atomic_init(&context.next_batch, 0);
    pthread_mutex_init(&context.mutex, NULL);
    pthread_cond_init(&context.cond, NULL);
    context.error = 0;
    context.error_message[0] = '\0';

    pthread_t *threads = calloc(worker_count, sizeof(*threads));
    if (threads == NULL) {
        pthread_mutex_destroy(&context.mutex);
        pthread_cond_destroy(&context.cond);
        fprintf(stderr, "out of memory while creating worker threads\n");
        return false;
    }

    bool ok = true;
    for (size_t index = 0; index < worker_count; index += 1) {
        if (pthread_create(&threads[index], NULL, worker_main, &context) != 0) {
            set_worker_error(&context, "failed to create worker thread");
            worker_count = index;
            ok = false;
            break;
        }
    }

    for (size_t batch_index = 0; batch_index < batch_count; batch_index += 1) {
        pthread_mutex_lock(&context.mutex);
        while (!context.batches[batch_index].ready && !context.error) {
            pthread_cond_wait(&context.cond, &context.mutex);
        }
        if (!context.batches[batch_index].ready) {
            ok = false;
            pthread_mutex_unlock(&context.mutex);
            break;
        }
        uint8_t *buffer = context.batches[batch_index].buffer;
        context.batches[batch_index].buffer = NULL;
        pthread_mutex_unlock(&context.mutex);

        if (fwrite(buffer, 1, context.batches[batch_index].byte_count, handle) != context.batches[batch_index].byte_count) {
            fprintf(stderr, "failed to write output batch %zu\n", batch_index);
            free(buffer);
            ok = false;
            break;
        }
        free(buffer);
    }

    for (size_t index = 0; index < worker_count; index += 1) {
        pthread_join(threads[index], NULL);
    }
    free(threads);

    if (context.error) {
        fprintf(stderr, "%s\n", context.error_message);
        ok = false;
    }

    for (size_t index = 0; index < batch_count; index += 1) {
        free(context.batches[index].buffer);
        context.batches[index].buffer = NULL;
    }

    pthread_mutex_destroy(&context.mutex);
    pthread_cond_destroy(&context.cond);
    return ok;
}

static bool write_condensed_overlap(const EntryArray *entries, const char *output_file) {
    if (!ensure_parent_dirs(output_file)) {
        fprintf(stderr, "failed to create output parent directories for %s: %s\n", output_file, strerror(errno));
        return false;
    }
    FILE *handle = fopen(output_file, "wb");
    if (handle == NULL) {
        fprintf(stderr, "failed to create %s: %s\n", output_file, strerror(errno));
        return false;
    }

    size_t worker_count = available_worker_count();
    if (entries->count > 1 && worker_count > entries->count - 1) {
        worker_count = entries->count - 1;
    }
    if (worker_count == 0) {
        worker_count = 1;
    }

    size_t batch_count = 0;
    RowBatch *batches = build_row_batches(entries->count, worker_count, &batch_count);
    if (entries->count > 1 && batches == NULL) {
        fprintf(stderr, "out of memory while preparing row batches\n");
        fclose(handle);
        return false;
    }

    bool ok;
    if (worker_count <= 1 || batch_count <= 1) {
        ok = write_batches_sequential(entries, batches, batch_count, handle);
    } else {
        ok = write_batches_parallel(entries, batches, batch_count, worker_count, handle);
    }
    free(batches);

    if (fclose(handle) != 0) {
        fprintf(stderr, "failed to flush %s: %s\n", output_file, strerror(errno));
        ok = false;
    }
    return ok;
}

static int load_entries_from_file(const char *input_file, EntryArray *entries) {
    size_t length = 0;
    char *contents = read_text_file(input_file, &length);
    if (contents == NULL) {
        fprintf(stderr, "failed to read %s: %s\n", input_file, strerror(errno));
        return 1;
    }

    Parser parser;
    parser.path = input_file;
    parser.cur = contents;
    parser.end = contents + length;
    parser.error[0] = '\0';

    memset(entries, 0, sizeof(*entries));
    bool ok = parse_entries(&parser, entries);
    skip_ws(&parser);
    if (ok && parser.cur != parser.end) {
        parser_error(&parser, "unexpected trailing content in %s", input_file);
        ok = false;
    }
    if (!ok) {
        fprintf(stderr, "%s\n", parser.error);
        free_entries(entries);
        free(contents);
        return 1;
    }

    free(contents);
    return 0;
}

int main(int argc, char **argv) {
    Args args;
    if (parse_args(argc, argv, &args) != 0) {
        return 1;
    }

    EntryArray entries;
    if (load_entries_from_file(args.input_file, &entries) != 0) {
        return 1;
    }

    if (entries.count < 2) {
        fprintf(
            stderr,
            "need at least two non-empty interface_msa_columns_a entries, found %zu\n",
            entries.count
        );
        free_entries(&entries);
        return 1;
    }

    if (!write_condensed_overlap(&entries, args.output_file)) {
        free_entries(&entries);
        return 1;
    }

    if (args.metadata_out != NULL && !write_metadata_file(&entries, args.metadata_out)) {
        free_entries(&entries);
        return 1;
    }

    fprintf(
        stderr,
        "wrote %zu entries and %zu distances to %s\n",
        entries.count,
        condensed_size(entries.count),
        args.output_file
    );

    free_entries(&entries);
    return 0;
}
