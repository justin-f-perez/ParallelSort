Please turn in your code on ICON along with a document answering the following questions:

## Implementation

I employ a divide-and-conquer strategy, splitting the sorting problem into independent sub-problems, by:
* slicing the input into multiple (non-overlapping) chunks
* spawning worker threads to sort each chunk independently (ensuring no chunk is concurrently accessed by more than one thread at a time)
* joining the worker threads
* merging the (locally) sorted chunks via merge-sort

Spawning threads that do no work *might* fit the assignment requirements by a narrow, literal interpretation- but not in spirit. For example, in the case that there is only one chunk to be sorted among a group of three worker threads, only one of them would ever do any work. Thus, not only can chunks be sorted in parallel, but for a multi-way merge sort where the number of active threads is fixed, they must be done in parallel. Merging can also be performed in parallel if this merge-sort algorithm is applied recursively.

We can't just recurse until chunk size is 1 byte, though!  Contrary to popular belief among middle management at retail outlets everywhere, creating more work for your alredy saturated workers does not make them work faster. CPU cycles are a finite resource, as is the number of idle cores new threads can take advantage of. Every cycle spent instantiating new threads or data structures to represent a chunk sorting task is a cycle that can't be spent on making a comparison. Furthermore, these objects have a memory cost that is realized on the JVM heap (risking an OOM crash for large inputs), the stack (risking stack overflow), or non-volatile storage (consuming I/O bandwidth, which is a hardware-based bottleneck on many systems). Thus, relative system overhead increases as chunk size decreases- even when the number of threads remains fixed.

The problem of bottlenecking on non-volatile storage I/O is non-trivial, in part because of how little is known about the target platform/hardware that this program will be evaluated on (but not for a lack of trying, most of my questions along this line didn't get a response.) "In the real world" software specifications include target platforms, supported dependency versions, and minimum hardware requirements. I don't even know what version of java the program will run against or how much physical memory is available, which also means it's not possible to determine what the default parameters given to the JVM (including max heap size) will be. Furthermore, a rotating hard disk, for example, has poor random read/write speed and sequential read/write throughput. SSDs fare much better for random access and throughput. State of the art NVMe drives are all but guaranteed to not be a bottleneck. 

After toiling with this I/O problem for many hours, a solution to the I/O problem presented itself: memory mapped files. Memory mapped byte buffers map directly to virtual memory (allocated off of the Java heap). The operating system handles reading pages from the mapped regions of the file into physical memory (page cache) and evicting them from cache (writing changes back to disk) as needed. This mechanism provides the program with the benefit of a much larger memory space to work with and leverages the operating system's highly efficient read/write caching.

My implementation creates separate memory maps for each chunk of the input file. Chunk sorting is done in-memory by allocating a chunk-sized array on the heap, invoking Java's builtin Arrays.sort() on the array, and then pushing the sorted chunk into an equivalent (in terms of position and size) chunk in a temporary (scratch space) file. This in-memory chunk sorting is why it's important that chunks be small enough we can hold as many chunks as we have threads in memory at the same time. Since each chunk is sorted by exactly one thread, and there are no overlaps between chunks, data races are not possible. Only after (1) all chunks have been sorted, (2) all worker threads have joined the main thread, and (3) the input and scratch files have been closed, the main thread re-opens the scratch file (and remaps the chunks into memory) in addition to creating one memory map for the output file. The main thread then merges the chunks' byte buffers (which are wrapped in LongBuffers) into the output file by using a PriorityQueue as a min-heap. The chunks' buffers are added directly to the PriorityQueue "min-heap", which compares them by the value of the element at their current `position`. In pseudo-code:

```
let input_buffers be the set of LongBuffers backed by byte buffers memory mapped to the scratch file (each buffer is a "chunk" that is already sorted)
let output_buffer be the singular LongBuffer backed by a byte buffer memory mapped to the output file
let min_heap be the PriorityQueue acting as a min-heap

LOOP:
for each buffer in input_buffers:
    add buffer to min_heap
    
popped_buffer := min_heap.pop()
output_buffer.put(popped_buffer.get())  # side-effect: get() and put() update the `position` field of their respective buffers

if popped_buffer has remaining elements:
    add popped_buffer to min_heap again
    
if min_heap has remaining elements:
    GOTO LOOP
```

Although this merging algorithm uses multiple memory mapped byte buffers as input, the algorithm is only run on the main thread. Allocating threads to the chunk sorting tasks is handled by a FixedThreadPool, each sorting a mutually exclusive region of the input file into a mutually exclusive region of the scratch file, and only after joining these threads is merging performed.


## Evaluation

I wrote a JupyterLab notebook using a mixture of python and shell script to invoke the sorting program for 100 iterations on each combination of parameters prescribed in the problem statement, for a total of 3200 invocations on the final run (i.e., not including invocations from debugging). The notebook is included in the git repository hosted on GitHub, which contains many technical notes, including libraries used for the notebook, dependency versions, etc. Three interactive line charts including confidence interval bands are also present in the notebook. These charts plot:
1. real/wall-clock time
2. user CPU time
3. system CPU time

The repository can be found [here](https://github.com/justin-f-perez/ParallelSort). It's currently private, but will be made public following this assignment's due date. Alternatively, just send me your GitHub username and I'll grant private access. Static versions of the charts are presented below.

4. Given the collected data and the plots, discuss how your implementation scales. Your discussion should highlight:

a. What is the best-case scenario performance improvements that you would expect to see as the number of threads is increased?

b. If this best-case scenario is not achieved, describe the factors that contribute to the suboptimal performance improvement achieved by multi-threading.

c. What other optimizations might you include to improve the scalability of your approach further?

I would rewrite in a language that isn't Java, both for performance reasons, and because I find Java's bloated libraries and API design annoying and cumbersome. This causes major drag on my dev workflow, and makes iterating on and debugging programs that much slower.

For this particular problem (external sort)

5. Briefly state how much time you spent on the assignment and what you have learned.