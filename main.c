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

// this will change one recycling free pages is supported
uint32_t get_unused_page_num(Pager* pager) {
  return pager->num_pages;
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
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
  LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE
  + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;


// leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// split
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT =
  LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_LEFT_SPLIT_COUNT;

// internal node header layout
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
  INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE
  + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// internal node body layout
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3; // small number for testing

enum NodeType_t {
		 NODE_LEAF,
		 NODE_INTERNAL
};
typedef enum NodeType_t NodeType;

bool is_node_root(void* node) {
  uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
  return (bool) value;
}

void set_node_root(void* node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

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

uint32_t* leaf_node_next_leaf(void* node) {
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

NodeType get_node_type(void* node) {
  uint8_t* type = (uint8_t *)(node + NODE_TYPE_OFFSET);
  return (NodeType)(*type);
}

void set_node_type(void* node, NodeType type) {
  uint8_t value = type;
  *((uint8_t *)(node + NODE_TYPE_OFFSET)) = value;
}

uint32_t* internal_node_num_keys(void* node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
  return (void*) internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    return internal_node_right_child(node);
  } else {
    return internal_node_cell(node, child_num);
  }
}

uint32_t get_node_max_key(void* node) {
  switch (get_node_type(node)) {
  case (NODE_LEAF):
    return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  case (NODE_INTERNAL):
    return *internal_node_key(node, *internal_node_num_keys(node) - 1); // key numbers are 0 indexed
  }
}

uint32_t* node_parent(void* node) {
  return node + PARENT_POINTER_OFFSET;
}

void initialize_leaf_node(void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;
}

void initialize_internal_node(void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
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
    set_node_root(root_node, true);
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

// binary search
Cursor* leaf_node_find(Table* t, uint32_t page_num, uint32_t key) {
  void* node = get_page(t->pager, page_num);
  Cursor* c = malloc(sizeof(Cursor));
  c->table = t;
  c->page_num = page_num;

  uint32_t min_index = 0;
  uint32_t one_past_max_index = *leaf_node_num_cells(node);

  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key_at_index == key) {
      c->cell_num = index;
      return c;
    }
    if (key_at_index > key) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  c->cell_num = min_index;
  return c;
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
  // return index of the child which contains the given key
  uint32_t num_keys = *internal_node_num_keys(node);
  // binary search
  uint32_t min_index = 0;
  uint32_t max_index = num_keys; // child count is 1 more than keys
  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if (key <= key_to_right) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }
  return min_index;
}

Cursor* internal_node_find(Table* t, uint32_t page_num, uint32_t key) {
  void* node = get_page(t->pager, page_num);
  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_page_num = *internal_node_child(node, child_index);
  void* child = get_page(t->pager, child_page_num);
  switch (get_node_type(child)) {
  case NODE_LEAF:
    return leaf_node_find(t, child_page_num, key);
  case NODE_INTERNAL:
    return internal_node_find(t, child_page_num, key);
  }
}


// returns position where key can be inserted, if already present,
// returns its position
Cursor* table_find(Table* t, uint32_t key) {
  void* root_node = get_page(t->pager, t->root_page_num);
  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(t, t->root_page_num, key);
  } else {
    return internal_node_find(t, t->root_page_num, key);
  }
}

Cursor* table_start(Table* t) {
  Cursor* cursor = table_find(t, 0);
  void* node = get_page(t->pager, cursor->page_num);
  cursor->end_of_table = (*leaf_node_num_cells(node) == 0);
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
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      c->end_of_table = true;
    } else {
      c->page_num = next_page_num;
      c->cell_num = 0;
    }
  }
}

void create_new_root(Table* t, uint32_t right_child_page_num) {
  // old root copied to new page, becomes left child
  void* root = get_page(t->pager, t->root_page_num);
  void* right_child = get_page(t->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(t->pager);
  void* left_child = get_page(t->pager, left_child_page_num);
  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);  // as whole root node is copied

  // root is not internal node
  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  *internal_node_key(root, 0) = get_node_max_key(left_child);
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = t->root_page_num;
  *node_parent(right_child) = t->root_page_num;
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

void internal_node_insert(Table* t, uint32_t parent_page_num, uint32_t child_page_num) {
  // add a new child-key pair to parent
  void* parent = get_page(t->pager, parent_page_num);
  void* child = get_page(t->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(child);
  uint32_t index = internal_node_find_child(parent, child_max_key);

  uint32_t original_num_keys = *internal_node_num_keys(parent);
  *internal_node_num_keys(parent) = original_num_keys + 1;
  if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
    printf("Need to implement splitting internal node\n");
    exit(EXIT_FAILURE);
  }

  uint32_t right_child_page_num = *internal_node_right_child(parent);
  void* right_child = get_page(t->pager, right_child_page_num);
  if (child_max_key > get_node_max_key(right_child)) {
    // if new child is going to be right child
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) = get_node_max_key(right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    // make room for new child
    for (uint32_t i = original_num_keys; i > index; i--) {
      void* destination = internal_node_cell(parent, i);
      void* source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}

void leaf_node_split_and_insert(Cursor* c, uint32_t key, Row* row) {
  // create new node, move upper half cell to it
  void* old_node = get_page(c->table->pager, c->page_num);
  uint32_t old_max = get_node_max_key(old_node);
  uint32_t new_page_num = get_unused_page_num(c->table->pager);
  void* new_node = get_page(c->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {  // using uint32_t here would cuase an infinite loop
    uint32_t j = (uint32_t) i;
    void* dest_node = j >= LEAF_NODE_LEFT_SPLIT_COUNT ? new_node : old_node;
    uint32_t index_in_node = j % LEAF_NODE_LEFT_SPLIT_COUNT;
    void* destination = leaf_node_cell(dest_node, index_in_node);

    if (j == c->cell_num) {
      *leaf_node_key(dest_node, index_in_node) = key;
      serialize_row(row, leaf_node_value(dest_node, index_in_node)); // 2nd arg can also be: destination + LEAF_NODE_KEY_SIZE
    } else if (j >= c->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, j - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, leaf_node_cell(old_node, j), LEAF_NODE_CELL_SIZE);
    }
  }

  *leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *leaf_node_num_cells(new_node) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) {
    return create_new_root(c->table, new_page_num);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(old_node);
    void* parent = get_page(c->table->pager, parent_page_num);
    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(c->table, parent_page_num, new_page_num);
    return;
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
		      EXECUTE_TABLE_FULL,
		      EXECUTE_DUPLICATE_KEY
};
typedef enum ExecuteResult_t ExecuteResult;

void leaf_node_insert(Cursor* c, uint32_t key, Row* row) {
  void* node = get_page(c->table->pager, c->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    leaf_node_split_and_insert(c, key, row);
    return;
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

void print_row(Row *row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

ExecuteResult execute_insert(Statement* s, Table* t) {
  void* node = get_page(t->pager, t->root_page_num);
  uint32_t num_cells = (*leaf_node_num_cells(node));

  Row* row = &(s->row);
  Cursor* cursor = table_find(t, row->id);

  if (cursor->cell_num < num_cells) {
    if ((*leaf_node_key(node, cursor->cell_num)) == row->id) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

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

void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf(" ");
  }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indent_level) {
  void* node = get_page(pager, page_num);
  uint32_t num_keys, child;
  switch (get_node_type(node)) {
  case NODE_LEAF:
    num_keys = *leaf_node_num_cells(node);
    indent(indent_level);
    printf("- leaf (size %d)\n", num_keys);
    for (uint32_t i = 0; i < num_keys; i++) {
      indent(indent_level + 1);
      printf("- %d\n", *leaf_node_key(node, i));
    }
    break;
  case NODE_INTERNAL:
    num_keys = *internal_node_num_keys(node);
    indent(indent_level);
    printf("- internal (size %d)\n", num_keys);
    for (uint32_t i = 0; i < num_keys; i++) {
      child = *internal_node_child(node, i);
      print_tree(pager, child, indent_level + 1);

      indent(indent_level + 1);
      printf("- key %d\n", *internal_node_key(node, i));
    }
    child = *internal_node_right_child(node);
    print_tree(pager, child, indent_level + 1);
    break;
  }
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* t) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(t);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_tree(t->pager, 0, 0);
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
    case (EXECUTE_DUPLICATE_KEY):
      printf("Error: Duplicate key.\n");
      break;
    }
  }

  return 0;
}
