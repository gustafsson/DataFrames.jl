# Container for a DataFrame row
immutable DataFrameRow{T <: AbstractDataFrame}
    df::T
    row::Int
end

function Base.getindex(r::DataFrameRow, idx::AbstractArray)
    return DataFrameRow(r.df[[idx]], r.row)
end

function Base.getindex(r::DataFrameRow, idx::Any)
    return r.df[r.row, idx]
end

function Base.setindex!(r::DataFrameRow, value::Any, idx::Any)
    return setindex!(r.df, value, r.row, idx)
end

Base.names(r::DataFrameRow) = names(r.df)
_names(r::DataFrameRow) = _names(r.df)

Base.sub(r::DataFrameRow, c) = DataFrameRow(r.df[[c]], r.row)

index(r::DataFrameRow) = index(r.df)

Base.length(r::DataFrameRow) = size(r.df, 2)

Base.endof(r::DataFrameRow) = size(r.df, 2)

Base.collect(r::DataFrameRow) = @compat Tuple{Symbol, Any}[x for x in r]

Base.start(r::DataFrameRow) = 1

Base.next(r::DataFrameRow, s) = ((_names(r)[s], r[s]), s + 1)

Base.done(r::DataFrameRow, s) = s > length(r)

Base.convert(::Type{Array}, r::DataFrameRow) = convert(Array, r.df[r.row,:])

# hash of DataFrame rows based on its values
# so that duplicate rows would have the same hash
function rowhash(df::DataFrame, r::Int, h::UInt = zero(UInt))
    @inbounds for col in columns(df)
        if isna(col, r)
            h = hash(false, h)
        else
            @inbounds h = hash(true, hash(col[r], h))
        end
    end
    return h
end

Base.hash(r::DataFrameRow, h::UInt) = rowhash(r.df, r.row, h)

# compare two elements in the array
_isequalelms(a::AbstractArray, i::Int, j::Int) = isequal(a[i], a[j])

# compare the two elements in the data array
function _isequalelms(a::DataArray, i::Int, j::Int)
    if isna(a, i)
        return isna(a, j)
    else
        return !isna(a, j) && isequal(a.data[i], a.data[j])
    end
end

# compare two elements in the pooled array
# NOTE assume there are no duplicated elements in the pool
_isequalelms(a::PooledDataArray, i::Int, j::Int) = isequal(a.refs[i], a.refs[j])

# comparison of DataFrame rows
function _isequal(df1::DataFrame, r1::Int, df2::DataFrame, r2::Int)
    if df1 !== df2
        if ncol(df1) != ncol(df2)
          throw(ArgumentError("Rows of the data frames that have different number of columns cannot be compared"))
        end
        for i in 1:ncol(df1)
          if !isequal(df1[r1,i], df2[r2,i])
            return false
          end
        end
        return true
    elseif r1 == r2 # same row
        return true
    else # different rows from the same frame
        for col in columns(df1)
            if !_isequalelms(col, r1, r2)
                return false
            end
        end
        return true
    end
end

# comparison of DataFrame rows
# rows are equal if they have the same values (while the row indices could differ)
Base.isequal(r1::DataFrameRow, r2::DataFrameRow) =
  _isequal(r1.df, r1.row, r2.df, r2.row)

# lexicographic ordering on DataFrame rows, NA < !NA
function Base.isless(r1::DataFrameRow, r2::DataFrameRow)
  if ncol(r1.df) != ncol(r2.df)
    throw(ArgumentError("Rows of the data frames that have different number of columns cannot be compared"))
  end
  for i in 1:ncol(r1.df)
    col1 = r1.df[i]
    col2 = r2.df[i]
    isna1 = isna(col1, r1.row)
    isna2 = isna(col2, r2.row)
    if isna1 != isna2
      return isna1 # NA < !NA
    elseif !isna1
      if isless(col1[r1.row], col2[r2.row])
        return true
      elseif !isequal(col1[r1.row], col2[r2.row])
        return false
      end
    end
  end
  return false
end
