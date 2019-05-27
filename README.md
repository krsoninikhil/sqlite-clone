# sqlite-clone
A clone of Sqlite database, follows this tutorial:
https://cstack.github.io/db_tutorial/

Each part of tutorial is covered in a separate commit. This database
has following functionalities:
- One predefined table.
- Persistent storage to a file.
- Data i.e. table rows stored as B-Tree.
- Support `SELECT` and `INSERT` statement, not in standard SQL format
  though. Example syntax can be seen in `main_spec.rb` file.
- Support meta-commands like `.exit` to save and exit, `.btree` to
  print underlying B-Tree.

# Build And Test
- Build binary and execute using:
  ```bash
  $ gcc main.c
  $ ./a.out test.db
  ```
- Test are written using `rspec` Ruby gem, which can be installed as:
  ```bash
  $ sudo gem install rspec
  ```
- Run test by:
  ```bash
  $ rspec main_spec.rb
  ```

# LICENSE
MIT License. License of original tutorial can be found
[here](https://github.com/cstack/db_tutorial/blob/master/LICENSE)
