# Mobile Computing HW1: ParallelSort
## Quick start
### Java
You can load this project in intellij to compile the java classes. Please don't commit your compiled output. There's an entrypoint in the ParallelExternalSorter class that provides its own default arguments (if you don't supply any). You can set a breakpoint here and hit debug if you just want to do a quick test to step through the code.

### Python Notebook
Make sure you've already built the Java project and then follow [this README](discussion/README.md).

## Contributing
### tips
* For simple editing of README it should be fine to just edit directly on the main branch in GitHub, but for everything else...

* Create your own development branch off of main.

* rebase your development branch onto main whenever there are new changes available (the longer you go between rebasing the more painful it will be to resolve merge conflicts)

### example:
```shell
git switch -c perez                 # creates a new development branch named "perez"

echo "making changes" >> README.md  # simulates changing some files

git fetch origin main               # updates origin/main branch

git rebase origin/main              # rebases commits of current branch (perez) onto origin/main branch

git push origin +perez              # pushes development branch to remote
                                    # the '+' means "force push"- because rebasing changes commit history.
                                    # this is fine to do when you're the only one using a branch.
                                    # NEVER FORCE PUSH MAIN or any other shared branch
                                    
open "https://github.com/justin-f-perez/ParallelSort/compare/main...perez"  # opens this URL in default browser (macOS)
```

## Description
As part of this programming assignment, you will be using threads to speed up the sorting of a (large) file.

## Requirements
### Constraints
* must implement sorting program in Java, using only packages that come with the standard installation of Java
* **no** constraints on order or range of long ints in input file, or sorting algorithm
* **no** constraints on sorting algorithm to be implemented

### Functional
1. The sorting program must accept two command-line arguments: 
    1. The name of an input file containing an array of long integers to be sorted
    2. The name of an output file that includes the sorted input array saved as an array of longs 

2. Measure and store the time it takes the sorting program to sort a file consisting of N={10^5, 10^6, 10^7, 10^8} long integers as you increase the number of threads T={2^0, 2^1, ... 2^7}.

3. Make a line plot such that:
    1. one line per n in N
    2. x=t for t in T
    3. y=time(n, t)

### Non-Functional
Answer in a text document:
1. Describe how you implemented your solution, highlighting the following aspects: 
    1. What actions can be taken in parallel? 
    2. What concurrency mechanisms have you used? 
    3. How did you make sure that your implementation has no race conditions? 
2. Include line plot
3. Discuss how your implementation scales. Your discussion should highlight: 
    1. What is the best-case scenario performance improvements that you would expect to see as the number of threads is increased? 
    2. If this best-case scenario is not achieved, describe the factors that contribute to the suboptimal performance improvement achieved by multi-threading. 
    3. What other optimizations might you include to improve the scalability of your approach further? 
4. Briefly state how much time you spent on the assignment and what you have learned.



## Evaluation 
* Submit code on ICON
* Answer and submit on ICON:

### Grading rubric
 5 points: Turned in the programming assignment
10 points: The program sorts the files correctly for a single thread
20 points: The program sorts the files correctly for multiple threads
20 points: The multi-threaded program runs faster than the single threaded program 
30 points: Answering the writeup questions
15 points: Ranking of your solution relative to other’s 


## Clarifications from professor:
### asked in class
> Can we use Arrays.sort()?

Yes, the point of the project isn't sorting, but it is about concurrency (so `Arrays.parallelSort()` is off the table)
> is 'name' the name of a file in the current working directory, a relative path, or an absolute path?

we can assume file is in current working directory -Justin
> what is the format of the input file's contents?

binary 64-bit long integers (as opposed to 64-bit long integers encoded as strings)

> how should the number of threads be handled if the only two inputs to the sorting algorithm are the input file path and output file path?

make the commandline argument for number of threads optional and set a default

### asked in email
> For the evaluation/ranking component of the assignment…
> * how much memory will be available to the JVM? (I’m mainly concerned with whether we can hold the input file in memory… I think the largest input is 64 * 10^8 bits, about 0.8 GB which is big enough to make me question it, and default JVM max heap sizes vary by implementation and available memory) 
> * what version of java and JVM will our code run against? (e.g. I’m currently using OpenJDK 16 & HotSpot VM.)
> * how will time be measured to ensure fairness? E.g.: NTP updates, other processes, cold vs warm CPU cache, page cache, etc.- can have a significant impact on performance.

"Don’t worry about tuning the VM. I will be using the default arguments. Yes, all the issues you mention impact performance, but we’re interested just in the average performance."

> The constraint that we use Java only applies to the concurrent sorting program, right? (e.g., use whatever we want for creating line plots is fine?)

"Any software would do." -Prof

_Technically, we don't even need to program the testing/plotting- for all he knows or cares, we could invoke by hand and do the plots in an Excel sheet. However, doing it by hand is a risk- if we discover some bug at the last minute and fix it, we would have to do all that work again by hand. Furthermore, automating it allows us to test and compare multiple implementations. -Justin_
