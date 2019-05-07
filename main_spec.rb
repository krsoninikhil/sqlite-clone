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
    script = (1..1401).map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    script << ".exit"
    result = run_scripts(script)
    expect(result[-2]).to eq('db > Error: Table full.')
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
                                    "leaf (size 3)",
                                    " - 0: 1",
                                    " - 1: 2",
                                    " - 2: 3",
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
end
