import os
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

db_url = os.environ["DB_URL"]
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()

from forum.models import Post, Comment

comments = (
   session.query(Comment.post_id, func.count("*").label("comments"))
   .group_by(Comment.post_id)
   .subquery()
)

negative_comments = (
   session.query(Comment.post_id, func.count("*").label("negative_comments"))
   .filter(Comment.sentiment == "negative")
   .group_by(Comment.post_id)
   .subquery()
)

positive_comments = (
   session.query(Comment.post_id, func.count("*").label("positive_comments"))
   .filter(Comment.sentiment == "positive")
   .group_by(Comment.post_id)
   .subquery()
)

final_query = (
   session.query(
       Post,
       comments.c.comments,
       negative_comments.c.negative_comments,
       positive_comments.c.positive_comments,
   )
   .outerjoin(comments, Post.id == comments.c.post_id)
   .outerjoin(negative_comments, Post.id == negative_comments.c.post_id)
   .outerjoin(positive_comments, Post.id == positive_comments.c.post_id)
)

items = []

for post, comments, negative_comments, positive_comments in final_query:
   items.append({
           "id": post.id,
           "body": post.body,
           "author_name": post.author_name,
           "created_on": str(post.created_on.date()),
           "comments": comments or 0,
           "positive_comments": positive_comments or 0,
           "negative_comments": negative_comments or 0,
       })

with open("forum_export.json", mode="w") as f:
   json.dump(items, f)
