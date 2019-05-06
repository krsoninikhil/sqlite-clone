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
  uint32_t num_pages;
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
  pager->num_pages = file_length / PAGE_SIZE;

  if (file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Corrupted file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n",
	   page_num, TABLE_MAX_PAGES);
  }
  if (pager->pages[page_num] == NULL) {
    // cache miss
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages_in_file = pager->file_length / PAGE_SIZE;
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
    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }
  return pager->pages[page_num];

}

void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush NULL page.\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_desc, pager->pages[page_num], PAGE_SIZE);
  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}


// BACK END

// common node header layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint32_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint32_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// leaf node header layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;


// leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// accessing leaf nodes
uint32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * cell_num;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}

void initialize_leaf_node(void* node) {
  *leaf_node_num_cells(node) = 0;
}

struct Table_t {
  uint32_t root_page_num;
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
  if (pager->num_pages == 0) {
    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
  }
  return t;
}

void db_close(Table* t) {
  Pager* pager = t->pager;
  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
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
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;
};
typedef struct Cursor_t Cursor;

Cursor* table_start(Table* t) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = t;
  cursor->page_num = t->root_page_num;
  cursor->cell_num = 0;

  void* root_node = get_page(t->pager, t->root_page_num);
  cursor->end_of_table = (*leaf_node_num_cells(root_node) == 0);
  return cursor;
}

Cursor* table_end(Table* t) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = t;
  cursor->end_of_table = true;
  cursor->page_num = t->root_page_num;

  void* root_node = get_page(t->pager, t->root_page_num);
  cursor->cell_num = *leaf_node_num_cells(root_node);
  return cursor;
}

void* cursor_value(Cursor* c) {
  void* page = get_page(c->table->pager, c->page_num);
  return leaf_node_value(page, c->cell_num);
}

void cursor_advance(Cursor* c) {
  void* node = get_page(c->table->pager, c->page_num);
  c->cell_num += 1;
  if (c->cell_num >= (*leaf_node_num_cells(node))) {
    c->end_of_table = true;
  }
}

void leaf_node_insert(Cursor* c, uint32_t key, Row* row) {
  void* node = get_page(c->table->pager, c->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    printf("Need to implement splitting a leaf node\n");
    exit(EXIT_FAILURE);
  }

  if (c->cell_num < num_cells) {
    // cell to insert row is already filled
    for (uint32_t i = num_cells; i > c->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1),
	     LEAF_NODE_CELL_SIZE);
    }
  }

  *leaf_node_key(node, c->cell_num) = key;
  *leaf_node_num_cells(node) += 1;
  serialize_row(row, leaf_node_value(node, c->cell_num));
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
  void* node = get_page(t->pager, t->root_page_num);
  if ((*leaf_node_num_cells(node)) >= LEAF_NODE_MAX_CELLS) {
    return EXECUTE_TABLE_FULL;
  }

  Cursor* cursor = table_end(t);
  leaf_node_insert(cursor, s->row.id, &(s->row));
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

void print_leaf_node(void* node) {
  uint32_t num_cells = *leaf_node_num_cells(node);
  printf("leaf (size %d)\n", num_cells);
  for (uint32_t i = 0; i < num_cells; i++) {
    printf(" - %d: %d\n", i, *leaf_node_key(node, i));
  }
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* t) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(t);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_leaf_node(get_page(t->pager, 0));
    return META_COMMAND_SUCCESS;
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
