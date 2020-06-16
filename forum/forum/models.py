from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True)
    body = Column(Text, nullable=False)
    author_name = Column(String(50), nullable=False)
    created_on = Column(TIMESTAMP)

    comments = relationship("Comment", back_populates="post")


class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)
    post_id = Column(Integer, ForeignKey("posts.id"))
    comment = Column(Text, nullable=False)
    sentiment = Column(String(10), nullable=False)
    commenter_name = Column(String(50), nullable=False)
    created_on = Column(TIMESTAMP)

    post = relationship("Post", back_populates="comments")