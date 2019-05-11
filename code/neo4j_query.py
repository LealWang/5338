from py2neo import Graph,NodeMatcher
#connect to neo4j
graph = Graph('http://127.0.0.1:7474',username='neo4j',password='123')
matcher = NodeMatcher(graph)
# for row in a:
#     print(row['u']['UserId'])

def query_sq1(question_id):
    '''
    find all users involved in a given question (identified by id) and their respective
    profiles including the creationDate, DisplayName, upVote and DownVote.
    :param question_id:the id of the question in posts collection
    :return: users involved
    '''
    query = "match (user:User)-[r:ASK|PROVIDE]->() where r.QuestionId="+str(question_id)+" return user"
    result = graph.run(query).data()
    return result

def query_sq2(topic):
    '''
    find the most viewed question in a given topic
    :param topic: topic name means tag name in the collection
    :return: the most viewed question in a given topic
    '''
    query = "match (question:Question)-[:TAGGED]->(tag:Tag{TagName:'"+\
            topic\
            +"'}) with max(question.ViewCount) as m match (question:Question{ViewCount:m}) return question"
    result = graph.run(query).data()
    return result

def query_aq3(topic):
    '''
    Given a topic, find the champion user and all questions the user has answers accepted in that topic.
    :param topic: topic name means tag name in the collection
    :return: question id and its title
    '''
    query = '''
    match (a:Answer)<-[r:ACCEPT]-(q:Question)-[:TAGGED]->(t:Tag) 
    where t.TagName= '%(topic)s'
    with a.OwnerUserId as user,count(r) as num with user,max(num) as max 
    order by max desc limit 1
    with user
    match (u:Answer{OwnerUserId:user})<-[r:ACCEPT]-(q:Question)-[tag_r:TAGGED]->(t:Tag{TagName: '%(topic)s'}) 
    return q.Id as Id,q.Title as Title
    order by q.Id asc 
    '''
    query = query % dict(topic=topic)
    result = graph.run(query).data()
    return result

def query_aq5(alpha):
    '''
    Discover questions with arguable accepted answer.
    :param alpha: upVote count greater than a given threshold value alpha
    :return: questions with arguable accepted answer.
    '''
    query = '''
    match (vq:Vote{VoteTypeId:2})-[rq:VOTE]->(q:Question)-[:ACCEPT]->()
    with q.Id as q_id,count(rq) as q_num
    where q_num > %(alpha)s
    with q_id
    match (vac:Vote{VoteTypeId:2})-[rac:VOTE]-(aac:Answer)<-[:ACCEPT]-(:Question{Id:q_id})
    with q_id,aac.Id as aac_id,count(rac) as rac_num
    match (a:Answer{ParentId:q_id})-[ac:VOTE]-(:Vote{VoteTypeId:2})
    with q_id,a.Id as a_id,count(ac) as ac_num,rac_num
    with q_id,max(ac_num) as ac_num,rac_num
    where ac_num > rac_num
    return q_id,ac_num as numOfMaxAnswerVote,rac_num as numOfAcceptedAnswerVote
    '''
    query = query % dict(alpha=alpha)
    result = graph.run(query).data()
    return result

def query_aq6(user_id):
    '''
    Discover the top five coauthors of a given user.
    :param user_id: user id
    :return: the top five coauthors of a given user.
    '''
    query = "match (a:Post)-[b:COOPERATE|ANSWER]->(c:Post) where a.OwnerUserId="+str(user_id)+\
            " return c.OwnerUserId as userId,count(c) as num order by num desc limit 5"
    result = graph.run(query).data()
    return result

print(query_aq5(10))