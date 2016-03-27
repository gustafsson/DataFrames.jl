module TestJoin
    using Base.Test
    using DataFrames

    name = DataFrame(Name = ["John Doe", "Jane Doe", "Joe Blogs"], ID = [1, 2, 3])
    job = DataFrame(ID = [1, 2, 2, 4], Job = ["Lawyer", "Doctor", "Florist", "Farmer"])

    # Join on symbols or vectors of symbols
    join(name, job, on = :ID)
    join(name, job, on = [:ID])
    # on is requied for any join except :cross
    @test_throws ArgumentError join(name, job)

    # Test output of various join types
    outer = DataFrame(Name = @data(["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", NA]),
                      ID = [1, 2, 2, 3, 4],
                      Job = @data(["Lawyer", "Doctor", "Florist", NA, "Farmer"]))

    # (Tests use current column ordering but don't promote it)
    right = outer[!isna(outer[:Job]), [:Name, :ID, :Job]]
    left = outer[!isna(outer[:Name]), :]
    inner = left[!isna(left[:Job]), :]
    semi = unique(inner[:, [:Name, :ID]])
    anti = left[isna(left[:Job]), [:Name, :ID]]

    @test isequal(join(name, job, on = :ID), inner)
    @test isequal(join(name, job, on = :ID, kind = :inner), inner)
    @test isequal(join(name, job, on = :ID, kind = :outer), outer)
    @test isequal(join(name, job, on = :ID, kind = :left), left)
    @test isequal(join(name, job, on = :ID, kind = :right), right)
    @test isequal(join(name, job, on = :ID, kind = :semi), semi)
    @test isequal(join(name, job, on = :ID, kind = :anti), anti)

    # Join with no non-key columns
    on = [:ID]
    nameid = name[on]
    jobid = job[on]

    @test isequal(join(nameid, jobid, on = :ID), inner[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :inner), inner[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :outer), outer[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :left), left[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :right), right[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :semi), semi[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :anti), anti[on])

    # Join using pooled vectors
    pname = DataFrame(Name = ["John Doe", "Jane Doe", "Joe Blogs"], ID = @pdata([1, 2, 3]))
    pjob = DataFrame(ID = @pdata([1, 2, 2, 4]), Job = ["Lawyer", "Doctor", "Florist", "Farmer"])
    pouter = DataFrame(Name = @data(["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", NA]),
                      ID = @pdata([1, 2, 2, 3, 4]),
                      Job = @data(["Lawyer", "Doctor", "Florist", NA, "Farmer"]))
    pright = pouter[!isna(pouter[:Job]), [:Name, :ID, :Job]]
    pleft = pouter[!isna(pouter[:Name]), :]
    pinner = pleft[!isna(pleft[:Job]), :]
    @test isequal(join(pname, pjob, on = :ID), pinner)
    @test isequal(join(pname, pjob, on = :ID, kind = :inner), pinner)
    @test isequal(join(pname, pjob, on = :ID, kind = :outer), pouter)
    @test isequal(join(pname, pjob, on = :ID, kind = :left), pleft)
    @test isequal(join(pname, pjob, on = :ID, kind = :right), pright)

    # Join on multiple keys
    df1 = DataFrame(A = 1, B = 2, C = 3)
    df2 = DataFrame(A = 1, B = 2, D = 4)

    @test isequal(join(df1, df2, on = [:A, :B]),
                  DataFrame(A = 1, B = 2, C = 3, D = 4))

    # Join on multiple keys with different order of "on" columns
    df1 = DataFrame(A = 1, B = :A, C = 3)
    df2 = DataFrame(B = :A, A = 1, D = 4)

    @test isequal(join(df1, df2, on = [:A, :B]),
                  DataFrame(A = 1, B = :A, C = 3, D = 4))

    # Test output of cross joins
    df1 = DataFrame(A = 1:2, B = 'a':'b')
    df2 = DataFrame(A = 1:3, C = 3:5)

    cross = DataFrame(A = [1, 1, 1, 2, 2, 2],
                      B = ['a', 'a', 'a', 'b', 'b', 'b'],
                      C = [3, 4, 5, 3, 4, 5])

    @test join(df1, df2[[:C]], kind = :cross) == cross

    # Cross joins handle naming collisions
    @test size(join(df1, df1, kind = :cross)) == (4, 4)

    # Cross joins don't take keys
    @test_throws ArgumentError join(df1, df2, on = :A, kind = :cross)

    # test empty inputs
    simple_df(len::Int, col=:A) = (df = DataFrame(); df[col]=collect(1:len); df)
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :left),  simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :left),  simple_df(2))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :left),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :right), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :right), simple_df(2))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :right), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :inner), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :inner), simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :inner), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :outer), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :outer), simple_df(2))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :outer), simple_df(2))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :semi),  simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :semi),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :semi),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :anti),  simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :anti),  simple_df(2))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :anti),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0, :B), kind = :cross), DataFrame(A=Int[], B=Int[]))
    @test isequal(join(simple_df(0), simple_df(2, :B), kind = :cross), DataFrame(A=Int[], B=Int[]))
    @test isequal(join(simple_df(2), simple_df(0, :B), kind = :cross), DataFrame(A=Int[], B=Int[]))
end
