namespace DotnetBroker.Core.Queue;

/// <summary>
/// Fixed-capacity, O(1) ring-buffer queue.
/// Thread-safe for concurrent Push + Peek using ReaderWriterLockSlim.
/// Pop is protected by a write-lock.
/// Absolute offsets are supported via <see cref="PopCount"/>.
/// </summary>
public sealed class RingBufferQueue<T>
{
    private readonly T?[] _arr;
    private readonly int  _capacity;
    private int           _head;   // index of the oldest element
    private int           _tail;   // index where next element goes
    private readonly ReaderWriterLockSlim _lock = new();

    public int  Count    { get; private set; }
    /// <summary>Total number of items ever popped. Used to resolve absolute offsets.</summary>
    public long PopCount { get; private set; }

    public RingBufferQueue(int capacity = 10_000)
    {
        _capacity = capacity;
        _arr      = new T?[capacity];
    }

    // ---- Write-side ----

    /// <summary>Push an item to the back. Throws if the buffer is full.</summary>
    public void PushBack(T item)
    {
        _lock.EnterWriteLock();
        try
        {
            if (Count == _capacity)
                throw new InvalidOperationException("Ring buffer is full — apply backpressure.");
            _arr[_tail] = item;
            _tail = (_tail + 1) % _capacity;
            Count++;
        }
        finally { _lock.ExitWriteLock(); }
    }

    /// <summary>Pop the front item. Returns default if empty.</summary>
    public T? PopFront()
    {
        _lock.EnterWriteLock();
        try
        {
            if (Count == 0) return default;
            var item = _arr[_head];
            _arr[_head] = default;
            _head = (_head + 1) % _capacity;
            Count--;
            PopCount++;
            return item;
        }
        finally { _lock.ExitWriteLock(); }
    }

    // ---- Read-side ----

    /// <summary>
    /// Peek at an absolute offset (i.e. the Nth message ever pushed, regardless of pops).
    /// Returns default if the message has been popped or hasn't arrived yet.
    /// </summary>
    public T? Peek(long absoluteOffset)
    {
        _lock.EnterReadLock();
        try
        {
            var relativePos = absoluteOffset - PopCount;
            if (relativePos < 0 || relativePos >= Count) return default;
            var idx = (_head + (int)relativePos) % _capacity;
            return _arr[idx];
        }
        finally { _lock.ExitReadLock(); }
    }

    /// <summary>
    /// Try-peek variant: returns true and sets <paramref name="item"/> if available.
    /// Avoids ambiguity with struct default values.
    /// </summary>
    public bool TryPeek(long absoluteOffset, out T item)
    {
        _lock.EnterReadLock();
        try
        {
            var relativePos = absoluteOffset - PopCount;
            if (relativePos < 0 || relativePos >= Count)
            {
                item = default!;
                return false;
            }
            var idx = (_head + (int)relativePos) % _capacity;
            item = _arr[idx]!;
            return true;
        }
        finally { _lock.ExitReadLock(); }
    }
}
