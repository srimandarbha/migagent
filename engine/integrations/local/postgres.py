import os
from ...contracts import *
from persistence.repository import SRETrackerRepository
from persistence.knowledge import PostgresVectorKnowledgeRepository

def build_postgres_services(dsn=None, embed=None):
    repo=SRETrackerRepository(dsn or os.getenv('DATABASE_URL','postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'))
    knowledge=PostgresVectorKnowledgeRepository(repo,embed=embed)
    return repo, knowledge
