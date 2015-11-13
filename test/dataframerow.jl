module TestDataFrameRow
    using Base.Test
    using DataFrames, Compat

    df = DataFrame(a=@data([1,   2,   3,   1,   2,   2 ]),
                   b=@data([2.0, NA,  1.2, 2.0, NA,  NA]),
                   c=@data(["A", "B", "C", "A", "B", NA]),
                   d=PooledDataArray(
                     @data([:A,  NA,  :C,  :A,  NA,  :C])))
    df2 = df[reverse(1:nrow(df)),:]
    df3 = DataFrame(a = @data([1, 2, 3]))

    # test the same frame
    #
    # Equality
    #
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 2))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 3))
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df, 4))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df, 5))
    @test !isequal(DataFrameRow(df, 2), DataFrameRow(df, 6))

    # isless()
    df4 = DataFrame(a=@data([1,   1,   2,   2,   2,   2  , NA,  NA]),
                    b=@data([2.0, 3.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0]),
                    c=@data([:B,  NA,  :A, :C,  :D,   :D,  :A,  :A ]))
    @test isless(DataFrameRow(df4, 1), DataFrameRow(df4, 2))
    @test !isless(DataFrameRow(df4, 2), DataFrameRow(df4, 1))
    @test !isless(DataFrameRow(df4, 1), DataFrameRow(df4, 1))
    @test isless(DataFrameRow(df4, 1), DataFrameRow(df4, 3))
    @test !isless(DataFrameRow(df4, 3), DataFrameRow(df4, 1))
    @test isless(DataFrameRow(df4, 3), DataFrameRow(df4, 4))
    @test !isless(DataFrameRow(df4, 4), DataFrameRow(df4, 3))
    @test isless(DataFrameRow(df4, 4), DataFrameRow(df4, 5))
    @test !isless(DataFrameRow(df4, 5), DataFrameRow(df4, 4))
    @test !isless(DataFrameRow(df4, 6), DataFrameRow(df4, 5))
    @test !isless(DataFrameRow(df4, 5), DataFrameRow(df4, 6))
    @test isless(DataFrameRow(df4, 7), DataFrameRow(df4, 8))
    @test !isless(DataFrameRow(df4, 8), DataFrameRow(df4, 7))
    @test !isless(DataFrameRow(df4, 1), DataFrameRow(df4, 8))
    @test isless(DataFrameRow(df4, 8), DataFrameRow(df4, 1))

    # hashing
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 2)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 3)))
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 4)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 5)))
    @test !isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 6)))

    # test compatible frames
    #
    # Equality
    #
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df2, 6))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df2, 5))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df2, 4))
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df2, 3))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df2, 2))
    @test !isequal(DataFrameRow(df, 2), DataFrameRow(df2, 1))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df2, 5))

    # hashing
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 6)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 5)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 4)))
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 3)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df2, 2)))
    @test !isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df2, 1)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df2, 5)))

    # check that hashrows() function generates the same hashes as DataFrameRow
    df_rowhashes = DataFrames.hashrows(df)
    @test df_rowhashes == [hash(dr) for dr in eachrow(df)]

    # test incompatible frames
    @test_throws ArgumentError isequal(DataFrameRow(df, 1), DataFrameRow(df3, 1))

    # test _RowGroupDict
    N = 20
    d1 = pdata(rand(@compat(map(Int64, 1:2)), N))
    df5 = DataFrame(Any[d1], [:d1])
    df6 = DataFrame(d1 = @pdata([2,3]))

    #test_group("groupby")
    gd = DataFrames._group_rows(df5)
    @test DataFrames.ngroups(gd) == 2
    #g_keys = sort!(collect(keys(gd)))
    #@test !isempty(gd[g_keys[1]])
    #@test length(gd[g_keys[1]]) + length(gd[g_keys[2]]) == N
    # getting groups for the rows of the other frames
    @test length(gd[DataFrameRow(df6, 1)]) > 0
    @test_throws KeyError gd[DataFrameRow(df6, 2)]
    @test isempty(get(gd, df6, 2))
    @test length(get(gd, df6, 2)) == 0
    # iterating over row group
#=  disabled, not supported
    c = 0
    for i in gd[g_keys[1]]
      @test 1 <= i <= nrow(df5)
      c += 1
    end
    @test c == length(gd[g_keys[1]])

    c = 0
    for i in gd[g_keys[2]]
      @test 1 <= i <= nrow(df5)
      c += 1
    end
    @test c == length(gd[g_keys[2]])

    # iterating over empty row group
    c = 0
    for i in get(gd, DataFrameRow(df6, 2))
      @test 1 <= i <= nrow(df5)
      c += 1
    end
    @test c == 0
=#

    # grouping empty frame
    gd = DataFrames._group_rows(DataFrame(x=Int[]))
    @test DataFrames.ngroups(gd) == 0

    # grouping single row
    gd = DataFrames._group_rows(df5[1,:])
    @test DataFrames.ngroups(gd) == 1
end
