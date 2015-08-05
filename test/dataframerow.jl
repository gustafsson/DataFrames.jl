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

    # test incompatible frames
    @test_throws ArgumentError isequal(DataFrameRow(df, 1), DataFrameRow(df3, 1))
end
