# Auto Complete Project

This is a project based on MapReduce that enables auto-conplete feature on user's input using N-Gram Model. It is mainly made of two MapReduce jobs.

## NGramLibraryBuilder (1st MapReduce job) :

* Input: All kinds of text files like words, phrases, sentences, paragraphs, articles and books (This time we are using the articles on Wikipedia)
* Processing: Parse the input into phrases of different length, from 2-gram to n-gram
* Output: Key-Value pairs, key is the different parsed phrases, values are their frequencies

## LanguageModel (2nd MapReduce job) : 
(Two jobs are connected by setting the 2nd's input to be 1st's output)

* Input: Output from former MapReduce processing, Key-Value pairs, key is the different parsed phrases, values are their frequencies
* Processing: Cut the key phrase into two parts, the first word to the second last word and the last word, then use the phrases' frequency to generate a arg that has three parts: starting_phrase, following_phrase(which will be the auto completion part), and the following_phrase's frequency
* Output: The DBOutWritable output to the database that has three parts: starting_phrase, following_phrase(which will be the auto completion part), and the following_phrase's frequency
