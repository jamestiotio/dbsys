# SUTD ISTD/CSD 2021 50.043 Database Systems Lab 2 Exercises

> James Raphael Tiovalen / 1004555

## Exercise 1

A new recording studio needs to maintain the following information:
- Each musician that records at Studify has an SSN, a name, and date of birth. Poorly paid musicians often live together at the same house which also has a phone.
- Each instrument used for recording at Studify has a unique identification number, a name (e.g., guitar, synthesizer, flute) and a musical key (e.g., C, B-flat, E-flat).
- Each album recorded has a unique identification number, a title, a copyright date, a format (e.g., CD or MC).
- Each song recorded has a unique identification number, a title, an author.
- Each musician may play several instruments, and a given instrument may be played by several musicians.
- Each album has a number of songs on it, but no song may appear on more than one album.
- Each song is performed by multiple musicians, and a musician may perform a number of songs.
- Each album has exactly one musician who acts as its producer. A musician may produce several albums.

ER Diagram:



## Exercise 2

You want to manage student clubs at the university. But you need to adhere to the following constraints:
- Each student has a unique student identification number, a name, and pillar.
- Each club has a unique name
- Every year, students can join a club. One student may belong to different clubs in different year, but may belong to at most one club in any given year.
- Each club may travel to different cities. Every city has a name, and belongs to a country
- Each club may travel to different cities in different year, but only to one city in any given year.

ER Diagram:



## Exercise 3

Translate this to relations:



## Exercise 4

Translate this to relations:



## Exercise 5

Given the following table which records the results of running competition at the Olympic:

Run(Name, Distance, Time)

Give an expression in relational algebra that finds all runners who:
- Take part in 100m category.
- Only take part in 100m category

## Exercise 6 - 9

Given the following relations modeling a library. A book in a library may have multiple copies.

Reader (ReaderID, FirstName, LastName)
Book (ISBN, Title, Author, PublicationDate, PublisherName)
Publisher (PublisherName, PublisherCity)
Copy(ISBN, CopyID, ShelfLocation)
Loan (ReaderID, ISBN, CopyID, ReturnDate)

Find:
- The names of readers who borrow more than 10 copies of a book.
- The title and author of book from publisher in London or New York.
- The title and author of books that Anh Dinh borrowed.
- The name of users who borrowed at least two different books.
