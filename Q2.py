from pyspark import SparkConf, SparkContext
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)

#Take a portion of ratings
portion=0.1
ratingsRDD = sc.textFile("ratings.csv").sample(False,portion,123)
tagsRDD=sc.textFile("tags.csv")
moviesRDD=sc.textFile("movies.csv")


#Pair up movie Id and name
def formatMovie(line):
    line=line.split(",")
    if line[1] !="movieId":
        mId=line[0]
        name=line[1]
        return[mId,name]
    else:
        return ["-1",-1] 

#Unite user id and movie id into an expression. Add rate as a float and form a tuple.
def formatRating(line):
    line=line.split(",")
    if line[0] !="movieId":
        user=line[0]
        mId=line[1]
        rate=float(line[2])
        
        return [user+":"+mId,rate]  
    else:
        return ["-1",-1]  

#Unite user  and movie id into an expression. Add tag and form a tuple.
def formatTag(line):
    line=line.split(",")
    if line[1] !="movieId":
        user=line[0]
        mId=line[1]
        tag=line[2]
        return [user+":"+mId,tag]  
    else:
        return ["-1",-1]  
 
#User is no longer needed after the union.   
def deleteUser(line):
    movie=line[0].split(":")[1]
    return (movie,line[1])
#Unite movie and tag this time.
def getMovieTag(line):
    return (line[0]+":"+line[1][1],line[1][0])
# Add 1 and form a tuple to count.
def countMovies(line):
    return (line[0],1)
# Split the expression.
def extractMovies(line):
    return (line[0].split(":")[0],(line[0].split(":")[1],line[1]))

def formatAll(line):
    return line[1]

#Make the tag the key of the line
def formatWTag(line):
    return (line[0],(line[1],line[2]))

#get max rating for a particular tag 
def getMaxRating(line):
    max=0.0
    maxName="Some Movie"
    for item in line[1]:
        if item[1]>max:
            max=item[1]
            maxName=item[0]
    return (line[0],maxName,max)


ratings= ratingsRDD.map(formatRating)

tags=tagsRDD.map(formatTag)


#Get Total Ratings
ratingTotal=ratings.join(tags).map(deleteUser).map(getMovieTag).reduceByKey(lambda x,y : x+y)\
.sortBy(lambda item : item[1],False)

#Get Counts
countTotal=ratings.join(tags).map(deleteUser).map(getMovieTag).map(countMovies)\
.reduceByKey(lambda x,y : x+y).sortBy(lambda item : item[1],False)
#Calculate the average by simple division
averageRating= ratingTotal.join(countTotal).map(lambda x: (x[0],x[1][0]/float(x[1][1]))).sortBy(lambda item : item[1],False)\
.map(extractMovies)
#Get the names of each movie
movies=moviesRDD.map(formatMovie)

#Join everything.  I retrieved movie names instead of just ids, too.
print averageRating.join(movies).map(formatAll).map(lambda x: (x[0][0],x[1],x[0][1]))\
.sortBy(lambda item : item[2],False).map(formatWTag).groupByKey().map(getMaxRating).sortBy(lambda item : item[2],False)\
.collect()
