from pyspark import SparkConf, SparkContext
sparkConf = SparkConf()
sc = SparkContext(conf=sparkConf)

#Take a portion of ratings
portion=0.006
ratingsRDD = sc.textFile("ratings.csv").sample(False,portion,123)

#Pair Up Movie and Rate Together
def formatRating(line):
    line=line.split(",")
    if line[0] !="movieId":
        user=line[0]
        mId=line[1]
        rate=line[2]
        
        return [mId+":"+rate,user]  
    else:
        return ["-1",-1]  

#Get Users that rated the same value to a particular movie   
def getUsers(line):
    myList=[]
    for user in line[1]:
        myList.append(user)
    return myList

#Before getting user pairs, sort the user id in order to impose a rule for order
def sortList(myList):
    secondList=[]
    for item in myList:
        secondList.append(int(item))
    secondList.sort()
    
    thirdList=[]
    
    for item in secondList:
        thirdList.append(str(item))
        
    return thirdList 

#Filter out single ones 
def checkLength(myList):
    if len(myList)>1:
        return True
    else:
        return False
    
#Set up the pairs
def getPairs(firstList):
    pairList=[]
        
    for ii in range(len(firstList)-1):
        for jj in range(ii+1,len(firstList)):
            pairList.append(firstList[ii]+":"+firstList[jj])
            
    return pairList
# Add 1 and form tuples   
def addOne(myList):
    secondList=[]
    for pair in myList:
        secondList.append((pair,1))
    return secondList
#Limit ratings to 100
def  limitTo100(myList):
    if myList[1]>100:
        myList=(myList[0],100)


    return myList
#print ratingsRDD.map(formatRating).groupByKey().flatMap(lambda x: x)\
#.reduceByKey(lambda x,y : x+y).collect()

#Take Out both movies
def takeOutUsers(line):
    user1=line[0].split(":")[0]
    user2=line[0].split(":")[1]
    similarity=line[1]
    return ([user1,[user2,similarity]],[user2,[user1,similarity]])

#Pair up users again

def getUserMatches(line):
    myArray=[]
    for item in line[1]:
        myPair=line[0]+":"+item[0]
        myArray.append([myPair,item[1]])
    return myArray
 
#Order the elements
def orderVector(line):
    
    for ii in range(len(line)):
        for jj in range(1,len(line)-ii):
            if line[jj-1][1]<line[jj][1] :
                temp=line[jj-1]
                line[jj-1]=line[jj]
                line[jj]=temp
    return line

#Limit to top ten for each user

def limitTopTen(line):
    user1=line[0][0].split(":")[0]
    userList=[]
        
    for item in line:
        userList.append(item[0].split(":")[1])
    
    number=10
    if(len(userList)<number):
        number=len(userList)
        
    userList=userList[0:number]
    
    return (user1,userList)
                   


print ratingsRDD.map(formatRating).groupByKey().map(getUsers).filter(checkLength).map(sortList)\
.map(getPairs).map(addOne).flatMap(lambda x: x).reduceByKey(lambda x,y : x+y).sortBy(lambda item : item[1],False)\
.map(limitTo100).map(takeOutUsers).flatMap(lambda x: x).groupByKey().\
map(getUserMatches).map(orderVector).map(limitTopTen).take(40)















