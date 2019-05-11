import pymongo_connection as connector
import datetime

conn = connector.MongodbConnector('localhost', 27017, 'assignment')
#return the result set by the given collection and condition
query_result = lambda collection, condition: [row for row in conn.get_collection(collection).aggregate(condition)]


def query_sq1(question_id):
    """
    find all users involved in a given question (identified by id) and their respective
    profiles including the creationDate, DisplayName, upVote and DownVote.
    :param question_id: the id of the question in posts collection
    :return: users involved
    """
    #find the specific question by question_id
    query = [
        {'$match': {'$or': [{'ParentId': question_id}, {'Id': question_id}],'OwnerUserId':{'$ne':""}}},
        {'$project': {'_id': 0, 'OwnerId': '$OwnerUserId'}}
             ]
    owners = [row['OwnerId'] for row in query_result('posts',query)]
    result = []
    #use answer_owner_id to find the info of user
    for owner_id in owners:
        query = [
            {'$match':{'Id': owner_id}},
            {'$project':{'_id':0,'DisplayName':1,'CreationDate':1,'UpVotes':1,'DownVotes':1}}
                 ]
        result.append(query_result('users',query))
    return result

def query_sq2(topic):
    """
    find the most viewed question in a given topic
    :param topic: topic name means tag name in the collection
    :return: the most viewed question in a given topic
    """
    query = [{'$match': {'Tags': topic,'PostTypeId':1}},{'$sort':{'ViewCount':-1}},{'$limit':1}]
    return query_result('posts',query)

def query_aq1(topics):
    """
    given a list of topics (tags), find the question easiest to answer in each topic.
    :param topics: a list of topics (tags),
    :return: question easiest to answer in each topic.
    """
    #format the query
    query = lambda topic:[
        {'$match':{'Tags':topic}},
        {'$lookup': {'from': 'posts', 'localField': 'AcceptedAnswerId', 'foreignField': 'Id', 'as': 'Answers'}},
        {'$unwind': '$Answers'},
        {'$project': {'_id': 0, 'Id':1,'Title': 1, 'Interval': {'$subtract': ['$Answers.CreationDate', '$CreationDate']}}},
        {'$sort':{'Interval':1}},
        {'$limit':1}
             ]
    return [query_result('posts',query(topic)) for topic in topics]

def query_aq4(user_id,alpha,date):
    """
    recommend unanswered questions to a given user.
    :param user_id: user id
    :param alpha: threshold value alpha
    :param date: cutoff  date of question creation
    :return: recommend unanswered questions
    """
    #find all topics that need to be recommended
    query = [
        {'$match':{'AcceptedAuthorId':user_id}},
        {'$unwind':'$Tags'},
        {'$group':{'_id':'$Tags','num':{'$sum':1}}},
        {'$sort':{'num':-1}},
        {'$match':{'num':{'$gte':alpha}}}
    ]
    topics = [topic['_id'] for topic in query_result('posts',query)]
    #reformat date type
    iso_date = datetime.datetime.strptime(date,'%Y-%m-%dT%H:%M:%S')
    query = [
        {'$match':{'PostTypeId':1,'AcceptedAuthorId':0,'Tags':{'$in':topics},'CreationDate':{'$lte':iso_date}}},
        {'$project':{'_id':0,'Id':1,'Title':1,'CreationDate':1}},
        {'$sort':{'CreationDate':-1}},
        {'$limit':5}
    ]
    return query_result('posts',query)

def query_aq2(start_date,end_date):
    """
    given a time period as indicated by starting and ending date,
    find the top 5 topics in that period.
    :param start_date: start date
    :param end_date: end date
    :return: the top 5 topics in the given period.
    """
    iso_start_date = datetime.datetime.strptime(start_date,"%Y-%m-%dT%H:%M:%S")
    iso_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
    query = [
        {'$match': {'CreationDate': {'$gte': iso_start_date, '$lte':iso_end_date}}},
        {'$unwind': '$Tags'},
        {'$group': {'_id': '$Tags', 'Users': {'$addToSet': '$OwnerUserId'}}},
        {'$project': {'_id':0,'Topic': '$_id', 'Num': {'$size': '$Users'}}},
        {'$sort': {'Num': -1}},
        {'$limit': 5}
    ]
    return query_result('posts',query)

print(query_aq4(1847,4,"2018-06-15T00:00:00"))
