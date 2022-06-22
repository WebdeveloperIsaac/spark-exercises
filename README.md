# Welcome!
This is an exercise repository for my spark course developed at msg systems ag.
See https://github.com/rondiplomatico/learn-spark for slides and coaching material.
You'll need an azure subscription to work with the cloud examples.

## Universe
We're up to crush some candy! Most of you probably know the game "CandyCrush", if not, check out https://www.king.com/de/game/candycrush for some fun. Warning - addictive potential!
So in this world it's about matching candies in order to crush them. Candies have Colors, Decoration and of course there's a person crushing the candy at a certain time at a certain place.
In this course, we use this setting as our main source of data.

## Stack
The examples are written in Java and we use maven to build and package.

## Exercise groups
The course is accompanied by a set of slides, introducing the material and describing the exercises.
Without them it's probably hard to understand what to do.

- FunctionalJava
  
  Basic examples to illustrate functional programming in plain java using the java streaming api
  
  See https://www.baeldung.com/java-8-streams or https://jenkov.com/tutorials/java-functional-programming/streams.html
- SparkBasic
  
  Implementing the exercises from FunctionalJava in Spark.
  
- SparkPersistence

  Basic examples on how to read and write Data or RDDs in Spark. Also connecting to an azure cloud storage!
  
- Spark Advanced
 
  Here we go into the more advanced paradigms when working with spark. Especially the execution model, shuffling and efficient implementations of common tasks are learned here.
  
- SparkStreaming

  Processing unbounded, continouous data with spark is possible using the Spark Structured Streaming API. Here we introduce some aspects of that.
  
  See https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  
# Disclaimer
This is illustrating material and is by no means intended to be used directly in any production environment. Use your head.


