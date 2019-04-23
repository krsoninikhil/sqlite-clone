#include <string.h>  // strcmp
#include <stdbool.h>  // for using true and false keyword
#include <stdlib.h>
#include <stdio.h>

// REPL

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

// COMPILER

enum PrepareResult_t {
		      PREPARE_SUCCESS,
		      PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t {
		      STATEMENT_INSERT,
		      STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

struct Statement_t {
  StatementType type;
};
typedef struct Statement_t Statement;

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    return PREPARE_SUCCESS;
  }

  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

// VM

enum MetaCommandResult_t {
			  META_COMMAND_SUCCESS,
			  META_COMMAND_UNRECOGNIZED
};
typedef enum MetaCommandResult_t MetaCommandResult;

void execute_statement(Statement* statement) {
  switch (statement->type) {
  case (STATEMENT_INSERT):
    printf("should be inserted here\n");
    break;
  case (STATEMENT_SELECT):
    printf("should do select here\n");
    break;
  }
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED;
  }
}


int main(int argc, char* argv[]) {
  InputBuffer* input_buffer = create_new_buffer();
  while (true) {
    print_promt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
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
    }

    execute_statement(&statement);
  }

  return 0;
}
