import random
from mimesis import Text

def generate_docs(count = 50):
    text = Text('ru')
    result = []
    for _ in range(count):
        result.append(text.text(quantity = 3))
    return result

def generate_tags(count = 10):
    text = Text('ru')
    return text.words(quantity = count)

def generate_relationship(docs, tags, max_tags = 5):
    result = {}
    for doc in docs:
        k = random.randint(0, max_tags)
        result[doc] = random.sample(tags, k)
    
    return result