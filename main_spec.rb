describe 'database' do
  before do
    `rm -rf test.db`
  end

  def run_scripts(commands)
    raw_output = nil
    IO.popen("./a.out test.db", "r+") do |pipe|
      commands.each do |command|
        begin
          pipe.puts command
        rescue Errno::EPIPE
          break
        end
      end

      pipe.close_write
      raw_output = pipe.gets(nil)
    end

    raw_output.split("\n")
  end

  it 'insert and retreives a row' do
    result = run_scripts(["insert 1 user1 u1@example.com", "select", ".exit"])
    expect(result).to match_array([
                                    "db > Executed.",
                                    "db > (1, user1, u1@example.com)",
                                    "Executed.",
                                    "db > ",
                                  ])
  end

  it 'prints error message when table is full' do
    script = (1..1401).map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    script << ".exit"
    result = run_scripts(script)
    expect(result.last(2)).to eq([
                                   "db > Executed.",
                                   "db > Need to implement splitting internal node",
                                 ])
  end

  it 'inserts strings of maximum length' do
    username = "a"*32
    email = "a"*255
    result = run_scripts(["insert 1 #{username} #{email}", "select", ".exit"])
    expect(result).to match_array([
                                    "db > Executed.",
                                    "db > (1, #{username}, #{email})",
                                    "Executed.",
                                    "db > ",
                                  ])
  end

  it 'prints error if strings are too long' do
    username = "a"*33
    email = "a"*256
    result = run_scripts(["insert 1 #{username} #{email}", "select", ".exit"])
    expect(result).to match_array([
                                    "db > String is too long.",
                                    "db > Executed.",
                                    "db > ",
                                  ])
  end

  it 'prints error if id is negative' do
    script = ["insert -1 user1 user1@example.com", "select", ".exit"]
    result = run_scripts(script)
    expect(result).to match_array([
                                    "db > ID must be positive.",
                                    "db > Executed.",
                                    "db > ",
                                  ])
  end

  it 'keeps data after closing connection' do
    result1 = run_scripts(["insert 1 user1 user1@example.com", ".exit"])
    expect(result1).to match_array([
                                     "db > Executed.",
                                     "db > ",
                                   ])

    result2 = run_scripts(["select", ".exit"])
    expect(result2).to match_array([
                                     "db > (1, user1, user1@example.com)",
                                     "Executed.",
                                     "db > ",
                                   ])
  end

  it 'prints the structure of one node tree' do
    scripts = [3, 1, 2].map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    scripts << ".btree"
    scripts << ".exit"
    result = run_scripts(scripts)

    expect(result).to match_array([
                                    "db > Executed.",
                                    "db > Executed.",
                                    "db > Executed.",
                                    "db > Tree:",
                                    "- leaf (size 3)",
                                    " - 1",
                                    " - 2",
                                    " - 3",
                                    "db > ",
                                  ])
  end

  it 'print error in case of duplicate id' do
    result = run_scripts([
                           "insert 1 u1 u1@example.com",
                           "insert 1 u2 u2@example.com",
                           "select",
                           ".exit"
                         ])
    expect(result).to match_array([
                                    "db > Executed.",
                                    "db > Error: Duplicate key.",
                                    "db > (1, u1, u1@example.com)",
                                    "Executed.",
                                    "db > ",
                                  ])
  end

  it 'print 3 leaf node btree' do
    scripts = (1..14).map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    scripts << ".btree"
    scripts << "insert 15 user15 user15@example.com"
    scripts << ".exit"
    result = run_scripts(scripts)

    expect(result[14..(result.length)])
      .to match_array([
                        "db > Tree:",
                        "- internal (size 1)",
                        " - leaf (size 7)",
                        "  - 1",
                        "  - 2",
                        "  - 3",
                        "  - 4",
                        "  - 5",
                        "  - 6",
                        "  - 7",
                        " - key 7",
                        " - leaf (size 7)",
                        "  - 8",
                        "  - 9",
                        "  - 10",
                        "  - 11",
                        "  - 12",
                        "  - 13",
                        "  - 14",
                        "db > Executed.",
                        "db > ",
                      ])
  end

  it 'prints all rows of multi-level btree' do
    script = (1..15).map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    script << "select"
    script << ".exit"
    result = run_scripts(script)

    expect(result[15..result.length]).to match_array([
                                    "db > (1, user1, user1@example.com)",
                                    "(2, user2, user2@example.com)",
                                    "(3, user3, user3@example.com)",
                                    "(4, user4, user4@example.com)",
                                    "(5, user5, user5@example.com)",
                                    "(6, user6, user6@example.com)",
                                    "(7, user7, user7@example.com)",
                                    "(8, user8, user8@example.com)",
                                    "(9, user9, user9@example.com)",
                                    "(10, user10, user10@example.com)",
                                    "(11, user11, user11@example.com)",
                                    "(12, user12, user12@example.com)",
                                    "(13, user13, user13@example.com)",
                                    "(14, user14, user14@example.com)",
                                    "(15, user15, user15@example.com)",
                                    "Executed.",
                                    "db > ",
                                  ])
  end

  it 'prints 4 leaf node btree' do
    # this test case input copied directly from tutorial the order of
    # insertion is such that, tree will split into 4 leaf node
    scripts = [
      "insert 18 user18 person18@example.com",
      "insert 7 user7 person7@example.com",
      "insert 10 user10 person10@example.com",
      "insert 29 user29 person29@example.com",
      "insert 23 user23 person23@example.com",
      "insert 4 user4 person4@example.com",
      "insert 14 user14 person14@example.com",
      "insert 30 user30 person30@example.com",
      "insert 15 user15 person15@example.com",
      "insert 26 user26 person26@example.com",
      "insert 22 user22 person22@example.com",
      "insert 19 user19 person19@example.com",
      "insert 2 user2 person2@example.com",
      "insert 1 user1 person1@example.com",
      "insert 21 user21 person21@example.com",
      "insert 11 user11 person11@example.com",
      "insert 6 user6 person6@example.com",
      "insert 20 user20 person20@example.com",
      "insert 5 user5 person5@example.com",
      "insert 8 user8 person8@example.com",
      "insert 9 user9 person9@example.com",
      "insert 3 user3 person3@example.com",
      "insert 12 user12 person12@example.com",
      "insert 27 user27 person27@example.com",
      "insert 17 user17 person17@example.com",
      "insert 16 user16 person16@example.com",
      "insert 13 user13 person13@example.com",
      "insert 24 user24 person24@example.com",
      "insert 25 user25 person25@example.com",
      "insert 28 user28 person28@example.com",
      ".btree",
      ".exit",
    ]
    result = run_scripts(scripts)
    expected = [
      "db > Tree:",
      "- internal (size 3)",
      " - leaf (size 7)",
      "  - 1",
      "  - 2",
      "  - 3",
      "  - 4",
      "  - 5",
      "  - 6",
      "  - 7",
      " - key 7",
      " - leaf (size 8)",
      "  - 8",
      "  - 9",
      "  - 10",
      "  - 11",
      "  - 12",
      "  - 13",
      "  - 14",
      "  - 15",
      " - key 15",
      " - leaf (size 7)",
      "  - 16",
      "  - 17",
      "  - 18",
      "  - 19",
      "  - 20",
      "  - 21",
      "  - 22",
      " - key 22",
      " - leaf (size 8)",
      "  - 23",
      "  - 24",
      "  - 25",
      "  - 26",
      "  - 27",
      "  - 28",
      "  - 29",
      "  - 30",
      "db > ",
    ]

    expect(result[30..(result.length)]).to match_array(expected)
  end
end
