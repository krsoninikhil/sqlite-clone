#include <string.h>  // strcmp
#include <stdbool.h>  // for using true and false keyword
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>  // some functions set `errno` in case of errors
#include <unistd.h>  // file I/O
#include <fcntl.h>  // for using file control options

// CORE: INTERACE / REPL

struct InputBuffer_t {
  char* buffer;
  size_t buffer_length;
  size_t input_length;
};
typedef struct InputBuffer_t InputBuffer;  // to avoid using struct keyword everytime

InputBuffer* create_new_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void close_input_buffer(InputBuffer* input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

void print_promt() {
  printf("db > ");
}

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read =
    getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  // ingore new line
  input_buffer->buffer[bytes_read - 1] = 0;
  input_buffer->input_length = bytes_read - 1;
}

// CORE: SQL COMMAND PROCESSOR

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
#define size_of_attr(type, attr) sizeof(((type*)0)->attr)

struct Row_t {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
};
typedef struct Row_t Row;

const uint32_t ID_SIZE = size_of_attr(Row, id);
const uint32_t USERNAME_SIZE = size_of_attr(Row, username);
const uint32_t EMAIL_SIZE = size_of_attr(Row, email);
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

enum PrepareResult_t {
		      PREPARE_SUCCESS,
		      PREPARE_UNRECOGNIZED_STATEMENT,
		      PREPARE_SYNTAX_ERROR,
		      PREPARE_STRING_TOO_LONG,
		      PREPARE_NEGATIVE_ID
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t {
		      STATEMENT_INSERT,
		      STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

struct Statement_t {
  StatementType type;
  Row row; // required for insert statement
};
typedef struct Statement_t Statement;

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* s) {
  s->type = STATEMENT_INSERT;
  char* keyword = strtok(input_buffer->buffer, " ");
  char* id_str = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

  if (id_str == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_str);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE
      || strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  s->row.id = id;
  strcpy(s->row.username, username);
  strcpy(s->row.email, email);
  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* s) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, s);
  }

  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    s->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}


// BACK END: PAGER

struct Pager_t {
  int file_desc;
  uint32_t file_length;
  void* pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("Unable to open file.\n");
    exit(EXIT_FAILURE);
  }
  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_desc = fd;
  pager->file_length = file_length;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

void* get_page(Pager* pager, int page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n",
	   page_num, TABLE_MAX_PAGES);
  }
  if (pager->pages[page_num] == NULL) {
    // cache miss
    void* page = malloc(PAGE_SIZE);
    int num_pages_in_file = pager->file_length / PAGE_SIZE;
    if (pager->file_length % PAGE_SIZE) {
      num_pages_in_file += 1;
    }

    if (page_num <= num_pages_in_file) {
      lseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_desc, page, PAGE_SIZE);
      if (bytes_read == -1) {
	printf("Error reading file: %d\n", errno);
	exit(EXIT_FAILURE);
      }
    }
    pager->pages[page_num] = page;
  }
  return pager->pages[page_num];

}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush NULL page.\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_desc, pager->pages[page_num], size);
  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}


// BACK END

struct Table_t {
  uint32_t num_rows;
  Pager* pager;
};
typedef struct Table_t Table;

void serialize_row(Row* source, void* dest) {
  memcpy(dest + ID_OFFSET, &(source->id), ID_SIZE);
  strncpy(dest + USERNAME_OFFSET, source->username, USERNAME_SIZE);
  strncpy(dest + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void* source, Row* dest) {
  memcpy(&(dest->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(dest->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(dest->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  Table* t = malloc(sizeof(Table));
  t->pager = pager;
  t->num_rows = pager->file_length / ROW_SIZE;
  return t;
}

void db_close(Table* t) {
  Pager* pager = t->pager;
  uint32_t num_full_pages = t->num_rows / ROWS_PER_PAGE;
  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  // last partial page
  int num_additional_rows = t->num_rows % ROWS_PER_PAGE;
  if (num_additional_rows > 0) {
    pager_flush(pager, num_full_pages, num_additional_rows * ROW_SIZE);
    free(pager->pages[num_full_pages]);
    pager->pages[num_full_pages] = NULL;
  }


  int result = close(pager->file_desc);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }

  // this should not be required
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(t);
}

struct Cursor_t {
  Table* table;
  uint32_t row_num;
  bool end_of_table;
};
typedef struct Cursor_t Cursor;

Cursor* table_start(Table* t) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = t;
  cursor->row_num = 0;
  cursor->end_of_table = (t->num_rows == 0);
  return cursor;
}

Cursor* table_end(Table* t) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = t;
  cursor->row_num = t->num_rows;
  cursor->end_of_table = true;
  return cursor;
}

void* cursor_value(Cursor* c) {
  uint32_t page_num = c->row_num / ROWS_PER_PAGE;
  void* page = get_page(c->table->pager, page_num);
  uint32_t byte_offset = (c->row_num % ROWS_PER_PAGE) * ROW_SIZE;
  return page + byte_offset;
}

void cursor_advance(Cursor* c) {
  c->row_num += 1;
  if (c->row_num >= c->table->num_rows) {
    c->end_of_table = true;
  }
}


// CORE: VM

enum MetaCommandResult_t {
			  META_COMMAND_SUCCESS,
			  META_COMMAND_UNRECOGNIZED
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum ExecuteResult_t {
		      EXECUTE_SUCCESS,
		      EXECUTE_TABLE_FULL
};
typedef enum ExecuteResult_t ExecuteResult;

void print_row(Row *row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

ExecuteResult execute_insert(Statement* s, Table* t) {
  if (t->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Cursor* cursor = table_end(t);
  serialize_row(&(s->row), cursor_value(cursor));
  t->num_rows += 1;
  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* s, Table* t) {
  Row row;
  Cursor* cursor = table_start(t);
  while (!cursor->end_of_table) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* s, Table* t) {
  switch (s->type) {
  case (STATEMENT_INSERT):
    return execute_insert(s, t);
  case (STATEMENT_SELECT):
    return execute_select(s, t);
  }
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* t) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(t);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED;
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }
  Table *table = db_open(argv[1]);
  InputBuffer* input_buffer = create_new_buffer();
  while (true) {
    print_promt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
      case (META_COMMAND_SUCCESS):
	continue;
      case (META_COMMAND_UNRECOGNIZED):
	printf("Unrecognized command '%s'.\n", input_buffer->buffer);
	continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
    case (PREPARE_SUCCESS):
      break;
    case (PREPARE_UNRECOGNIZED_STATEMENT):
      printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer);
      continue;
    case (PREPARE_SYNTAX_ERROR):
      printf("Syntax Error. Could not parse query.\n");
      continue;
    case (PREPARE_STRING_TOO_LONG):
      printf("String is too long.\n");
      continue;
    case (PREPARE_NEGATIVE_ID):
      printf("ID must be positive.\n");
      continue;
    }

    switch (execute_statement(&statement, table)) {
    case (EXECUTE_SUCCESS):
      printf("Executed.\n");
      break;
    case (EXECUTE_TABLE_FULL):
      printf("Error: Table full.\n");
      break;
    }
  }

  return 0;
}
