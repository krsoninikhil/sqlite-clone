describe 'database' do
  before do
    `rm -rf test.db`
  end

  def run_scripts(commands)
    raw_output = nil
    IO.popen("./a.out test.db", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
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
    script = (1..15).map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    script << ".exit"
    result = run_scripts(script)
    expect(result.last(2)).to eq([
                                   "db > Executed.",
                                   "db > Need to implement searching an internal node.",
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
                        "db > Need to implement searching an internal node.",
                      ])
  end
end
