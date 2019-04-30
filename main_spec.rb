describe 'database' do
  def run_scripts(commands)
    raw_output = nil
    IO.popen("./a.out", "r+") do |pipe|
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
end
