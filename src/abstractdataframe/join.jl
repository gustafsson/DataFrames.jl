@comment """
# Joins
"""

##
## Join / merge
##

# helper structure for dataframes joining
immutable _DataFrameJoiner{DF1<:AbstractDataFrame, DF2<:AbstractDataFrame}
    dfl::DF1
    dfr::DF2
    dfl_on::DF1
    dfr_on::DF2
    on_cols::Vector{Symbol}

    function _DataFrameJoiner(dfl::DF1, dfr::DF2, on::@compat(Union{Symbol,Vector{Symbol}}))
        on_cols = (isa(on, Symbol) ? fill(on::Symbol, 1) : on)::Vector{Symbol}
        new(dfl, dfr, dfl[on_cols], dfr[on_cols], on_cols)
    end
end

_DataFrameJoiner{DF1<:AbstractDataFrame, DF2<:AbstractDataFrame}(dfl::DF1, dfr::DF2, on::@compat(Union{Symbol,Vector{Symbol}})) =
    _DataFrameJoiner{DF1,DF2}(dfl, dfr, on)

# helper map between the row indices in original and joined frame
immutable _RowIndexMap
  orig::Vector{Int} # row indices in the original frame
  join::Vector{Int} # row indices in the resulting joined frame
end

Base.length(x::_RowIndexMap) = length(x.orig)

# composes the joined data frame using the maps between the left and right
# frame rows and the indices of rows in the result
function _compose_joined_frame(joiner::_DataFrameJoiner,
                  left_ixs::_RowIndexMap, leftonly_ixs::_RowIndexMap,
                  right_ixs::_RowIndexMap, rightonly_ixs::_RowIndexMap)
    @assert length(left_ixs) == length(right_ixs)
    # compose left half of the result taking all left columns
    # complicated way to do vcat that avoids expensive setindex!() for PooledDataVector
    all_orig_left_ixs = [left_ixs.orig; leftonly_ixs.orig]
    if length(leftonly_ixs) > 0
      # permute the indices to restore left frame rows order
      all_orig_left_ixs[[left_ixs.join; leftonly_ixs.join]] = all_orig_left_ixs
    end
    left_nas = fill(NA, length(rightonly_ixs))
    left_df = DataFrame(Any[append!(col[all_orig_left_ixs], left_nas)
                            for col in columns(joiner.dfl)],
                        names(joiner.dfl))

    # compose right half of the result taking all right columns excluding on
    dfr_noon = without(joiner.dfr, joiner.on_cols)
    # complicated way to do vcat that avoids expensive setindex!() for PooledDataVector
    right_nas = fill(NA, length(leftonly_ixs))
    # permutation to swap rightonly and leftonly rows
    right_perm = [1:length(right_ixs);
                  ((length(right_ixs)+length(rightonly_ixs)+1):
                   (length(right_ixs)+length(rightonly_ixs)+length(leftonly_ixs)));
                  ((length(right_ixs)+1):(length(right_ixs)+length(rightonly_ixs)))]
    if length(leftonly_ixs) > 0
      # compose right_perm with the permutation that restores left rows order
      right_perm[[right_ixs.join; leftonly_ixs.join]] = right_perm[1:(length(right_ixs)+length(leftonly_ixs))]
    end
    all_orig_right_ixs = [right_ixs.orig; rightonly_ixs.orig]
    right_df = DataFrame(Any[append!(col[all_orig_right_ixs], right_nas)[right_perm]
                             for col in columns(dfr_noon)],
                         names(dfr_noon))
    # merge left and right parts of the joined frame
    res = hcat!(left_df, right_df)

    if length(rightonly_ixs.join) > 0
      # some left rows are NA, so the values of the "on" columns
      # need to be taken from the right
      for on_col in joiner.on_cols
        if isa(res[on_col], PooledDataVector)
          # since setindex!() for PoolDataArray is very slow,
          # it requires special handling
          # merge the pools
          left_on_col, right_on_col = PooledDataVecs(joiner.dfl_on[on_col], joiner.dfr_on[on_col])
          # rebuild the result column
          res[on_col] = PooledDataArray(
              DataArrays.RefArray([left_on_col.refs[all_orig_left_ixs];
                                   right_on_col.refs[rightonly_ixs.orig]]),
                  left_on_col.pool)
        else
          res[rightonly_ixs.join, on_col] = joiner.dfr_on[rightonly_ixs.orig, on_col]
        end
      end
    end
    return res
end

# map the indices of the left and right joined frames
# to the indices of the rows in the resulting frame
# if `nothing` is given, the corresponding map is not built
function _map_rows!(left_frame::AbstractDataFrame, right_frame::AbstractDataFrame,
                   right_dict::_RowGroupDict,
                   left_ixs::@compat(Union{Void, _RowIndexMap}),
                   leftonly_ixs::@compat(Union{Void, _RowIndexMap}),
                   right_ixs::@compat(Union{Void, _RowIndexMap}),
                   rightonly_mask::@compat(Union{Void, Vector{Bool}}))
    # helper functions
    update!(ixs::@compat(Void), orig_ix::Int, join_ix::Int, count::Int = 1) = ixs
    function update!(ixs::_RowIndexMap, orig_ix::Int, join_ix::Int, count::Int = 1)
        for i in 1:count
            push!(ixs.orig, orig_ix)
        end
        for i in join_ix:(join_ix+count-1)
            push!(ixs.join, i)
        end
        ixs
    end
    update!(ixs::@compat(Void), orig_ixs::AbstractArray, join_ix::Int) = ixs
    function update!(ixs::_RowIndexMap, orig_ixs::AbstractArray, join_ix::Int)
        append!(ixs.orig, orig_ixs)
        for i in join_ix:(join_ix+length(orig_ixs)-1)
            push!(ixs.join, i)
        end
        ixs
    end
    update!(ixs::@compat(Void), orig_ixs::AbstractArray) = ixs
    update!(mask::Vector{Bool}, orig_ixs::AbstractArray) = (mask[orig_ixs] = false)

    # iterate over left rows and compose the left<->right index map
    next_join_ix = 1
    for l_row in eachrow(left_frame)
        r_ixs = get(right_dict, l_row)
        if isempty(r_ixs)
            update!(leftonly_ixs, l_row.row, next_join_ix)
            next_join_ix += 1
        else
            update!(left_ixs, l_row.row, next_join_ix, length(r_ixs))
            update!(right_ixs, r_ixs, next_join_ix)
            update!(rightonly_mask, r_ixs)
            next_join_ix += length(r_ixs)
        end
    end
end

# map the row indices of the left and right joined frames
# to the indices of rows in the resulting frame
# returns the 4-tuple of row indices maps for
# - matching left rows
# - non-matching left rows
# - matching right rows
# - non-matching right rows
# if false is provided, the corresponding map is not built and the
# tuple element is empty _RowIndexMap
function _map_rows(
    left_frame::AbstractDataFrame, right_frame::AbstractDataFrame,
    right_dict::_RowGroupDict,
    map_left::Bool, map_leftonly::Bool,
    map_right::Bool, map_rightonly::Bool)
    init_map(df::AbstractDataFrame, init::Bool) = init ?
        _RowIndexMap(sizehint!(@compat(Vector{Int}()), nrow(df)),
                       sizehint!(@compat(Vector{Int}()), nrow(df))) :
         nothing
    to_bimap(x::_RowIndexMap) = x
    to_bimap(::@compat(Void)) = _RowIndexMap(@compat(Vector{Int}()), @compat(Vector{Int}()))

    # init maps as requested
    left_ixs = init_map(left_frame, map_left)
    leftonly_ixs = init_map(left_frame, map_leftonly)
    right_ixs = init_map(right_frame, map_right)
    if map_rightonly
        rightonly_mask = fill(true, nrow(right_frame))
    else
        rightonly_mask = nothing
    end
    _map_rows!(left_frame, right_frame, right_dict,
               left_ixs, leftonly_ixs, right_ixs, rightonly_mask)
    if map_rightonly
        rightonly_orig_ixs = (1:length(rightonly_mask))[rightonly_mask]
        rightonly_ixs = _RowIndexMap(rightonly_orig_ixs,
                         collect(length(right_ixs.orig)+
                                 (leftonly_ixs === nothing ? 0 : length(leftonly_ixs))+
                                 (1:length(rightonly_orig_ixs))))
    else
        rightonly_ixs = nothing
    end

    return to_bimap(left_ixs), to_bimap(leftonly_ixs),
           to_bimap(right_ixs), to_bimap(rightonly_ixs)
end

"""
Join two DataFrames

```julia
join(df1::AbstractDataFrame,
     df2::AbstractDataFrame;
     on::Union{Symbol, Vector{Symbol}} = Symbol[],
     kind::Symbol = :inner)
```

### Arguments

* `df1`, `df2` : the two AbstractDataFrames to be joined

### Keyword Arguments

* `on` : a Symbol or Vector{Symbol}, the column(s) used as keys when
  joining; required argument except for `kind = :cross`

* `kind` : the type of join, options include:

  - `:inner` : only include rows with keys that match in both `df1`
    and `df2`, the default
  - `:outer` : include all rows from `df1` and `df2`
  - `:left` : include all rows from `df1`
  - `:right` : include all rows from `df2`
  - `:semi` : return rows of `df1` that match with the keys in `df2`
  - `:anti` : return rows of `df1` that do not match with the keys in `df2`
  - `:cross` : a full Cartesian product of the key combinations; every
    row of `df1` is matched with every row of `df2`

`NA`s are filled in where needed to complete joins.

### Result

* `::DataFrame` : the joined DataFrame

### Examples

```julia
name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])

join(name, job, on = :ID)
join(name, job, on = :ID, kind = :outer)
join(name, job, on = :ID, kind = :left)
join(name, job, on = :ID, kind = :right)
join(name, job, on = :ID, kind = :semi)
join(name, job, on = :ID, kind = :anti)
join(name, job, kind = :cross)
```

"""
:join

function Base.join(df1::AbstractDataFrame,
                   df2::AbstractDataFrame;
                   on::@compat(Union{Symbol, Vector{Symbol}}) = Symbol[],
                   kind::Symbol = :inner)
    if kind == :cross
        if on != Symbol[]
            throw(ArgumentError("Cross joins don't use argument 'on'."))
        end
        return crossjoin(df1, df2)
    elseif on == Symbol[]
        throw(ArgumentError("Missing join argument 'on'."))
    end

    joiner = _DataFrameJoiner(df1, df2, on)

    if kind == :inner
        _compose_joined_frame(joiner, _map_rows(joiner.dfl_on, joiner.dfr_on,
                                   _group_rows(joiner.dfr_on),
                                   true, false, true, false)...)
    elseif kind == :left
        _compose_joined_frame(joiner, _map_rows(joiner.dfl_on, joiner.dfr_on,
                                   _group_rows(joiner.dfr_on),
                                   true, true, true, false)...)
    elseif kind == :right
        right_ixs, rightonly_ixs, left_ixs, leftonly_ixs =
            _map_rows(joiner.dfr_on, joiner.dfl_on,
                      _group_rows(joiner.dfl_on),
                      true, true, true, false)
        _compose_joined_frame(joiner, left_ixs, leftonly_ixs, right_ixs, rightonly_ixs)
    elseif kind == :outer
        _compose_joined_frame(joiner, _map_rows(joiner.dfl_on, joiner.dfr_on,
                                   _group_rows(joiner.dfr_on),
                                   true, true, true, true)...)
    elseif kind == :semi
        # hash the right rows
        right_rows = _unique_rows(joiner.dfr_on)
        # iterate over left rows and leave those found in right
        left_ixs = @compat Vector{Int}()
        sizehint!(left_ixs, nrow(joiner.dfl))
        for l_row in eachrow(joiner.dfl_on)
            if in(l_row, right_rows)
                push!(left_ixs, l_row.row)
            end
        end
        return joiner.dfl[left_ixs, :]
    elseif kind == :anti
        # hash the right rows
        right_rows = _unique_rows(joiner.dfr_on)
        # iterate over left rows and leave those not found in right
        leftonly_ixs = @compat Vector{Int}()
        sizehint!(leftonly_ixs, nrow(joiner.dfl))
        for l_row in eachrow(joiner.dfl_on)
            if !in(l_row, right_rows)
                push!(leftonly_ixs, l_row.row)
            end
        end
        return joiner.dfl[leftonly_ixs, :]
    else
        throw(ArgumentError("Unknown kind ($kind) of join requested"))
    end
end

function crossjoin(df1::AbstractDataFrame, df2::AbstractDataFrame)
    r1, r2 = size(df1, 1), size(df2, 1)
    cols = [[rep(c, 1, r2) for c in columns(df1)];
            [rep(c, r1, 1) for c in columns(df2)]]
    colindex = merge(index(df1), index(df2))
    DataFrame(cols, colindex)
end
