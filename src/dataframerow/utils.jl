# Rows grouping.
# Maps row contents to the indices of all the equal rows.
# Used by groupby(), join(), nonunique()
immutable _RowGroupDict{T<:AbstractDataFrame}
    indexer::Dict{DataFrameRow{T}, Int} # row contents => group index
    row_ixs::Vector{Int}  # permutation of row indices that sorts them by groups
    starts::Vector{Int}   # starts of ranges in row_ixs for each group
    stops::Vector{Int}    # stops of ranges in row_ixs for each group
end

Base.keys(group_dict::_RowGroupDict) = Base.keys(group_dict.indexer)
Base.length(group_dict::_RowGroupDict) = Base.length(group_dict.indexer)

function Base.getindex(group_dict::_RowGroupDict, r::DataFrameRow)
    g_ix = group_dict.indexer[r]
    sub(group_dict.row_ixs, group_dict.starts[g_ix]:group_dict.stops[g_ix])
end

function Base.get(group_dict::_RowGroupDict, r::DataFrameRow)
    g_ix = get(group_dict.indexer, r, 0)
    sub(group_dict.row_ixs, g_ix > 0 ? (group_dict.starts[g_ix]:group_dict.stops[g_ix]) : 0:-1)
end

# Builds _RowGroupDict for a given dataframe.
# Partly uses the code of Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).
function _group_rows{T<:AbstractDataFrame}(df::T)
    # translated from Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).
    # assign row group index to each row
    d = sizehint!(Dict{DataFrameRow{T}, Int}(), nrow(df))
    group_ixs = @compat Vector{Int}(nrow(df))
    for r in eachrow(df)
        group_ixs[r.row] = get!(d, r, length(d)+1)
    end

    # count elements in each group
    stops = zeros(Int, length(d))
    for g_ix in group_ixs
        stops[g_ix] += 1
    end

    # group start positions in a sorted frame
    starts = @compat Vector{Int}(length(d))
    if !isempty(starts)
      starts[1] = 1
      for i in 1:(length(d)-1)
          starts[i+1] = starts[i] + stops[i]
      end
    end

    # define row permutation that sorts them into groups
    row_ixs = @compat Vector{Int}(length(group_ixs))
    copy!(stops, starts)
    for i in 1:length(group_ixs)
        g_ix = group_ixs[i]
        row_ixs[stops[g_ix]] = i
        stops[g_ix] += 1
    end
    stops .-= 1
    return _RowGroupDict{T}(d, row_ixs, starts, stops)
end

# set of unique rows (value combinations) of a data frame
function _unique_rows{T<:AbstractDataFrame}(df::T)
    d = Set{DataFrameRow{T}}()
    sizehint!(d, nrow(df))
    for r in eachrow(df)
        push!(d, r)
    end
    return d
end
