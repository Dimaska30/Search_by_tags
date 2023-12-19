import sqlite3
import generate_data as Generator

class My_DB:
    
    def __init__(self):
        self.__connection = sqlite3.connect('my_database.db')
        self.__cursor = self.__connection.cursor()
        self.__create_doc_table__()
        self.__create_tags_table__()
        self.__create_relationship_table__()

    
    def __create_doc_table__(self):
        self.__cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS Docs (
                id_doc INTEGER PRIMARY KEY,
                doc TEXT NOT NULL
            )
            '''
        )
        self.__cursor.execute(
            '''
            SELECT id_doc FROM Docs
            '''
        )
        docs = self.__cursor.fetchall()
        if docs == []:
            sentances = Generator.generate_docs()
            for sentance in sentances:
                self.__cursor.execute('INSERT INTO Docs (doc) VALUES (?)', (sentance,))
    
    def __create_tags_table__(self):
        self.__cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS Tags (
                id_tag INTEGER PRIMARY KEY,
                tag TEXT NOT NULL
            )
            '''
        )
        self.__cursor.execute(
            '''
            SELECT id_tag FROM Tags
            '''
        )
        tags = self.__cursor.fetchall()
        if tags == []:
            words = Generator.generate_tags()
            for word in words:
                self.__cursor.execute('INSERT INTO Tags (tag) VALUES (?)', (word,))

    def __create_relationship_table__(self):
        self.__cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS Relationship (
                id INTEGER PRIMARY KEY,
                id_tag INTEGER REFERENCES Tags(id_tag) ON UPDATE CASCADE,
                id_doc INTEGER REFERENCES Docs(id_doc) ON UPDATE CASCADE
            )
            '''
        )
        self.__cursor.execute(
            '''
            SELECT id FROM Relationship
            '''
        )
        pairs = self.__cursor.fetchall()
        if pairs == []:
            self.__cursor.execute(
                '''
                SELECT id_doc FROM Docs
                '''
            )
            id_docs = self.__clean_result__(self.__cursor.fetchall())

            self.__cursor.execute(
                '''
                SELECT id_tag FROM Tags
                '''
            )
            id_tags = self.__clean_result__(self.__cursor.fetchall())
            my_pair_dict = Generator.generate_relationship(id_docs, id_tags)
            for doc in my_pair_dict.keys():
                for tag in my_pair_dict[doc]:
                    self.__cursor.execute('INSERT INTO Relationship (id_doc, id_tag) VALUES (?, ?)', (doc, tag))
      

    def get_cursor(self):
        return self.__cursor

    def get_doc_info(self, id):
        self.__cursor.execute(
                '''
                SELECT doc FROM Docs WHERE id_doc = (?)
                ''', (id,)
            )
        result = self.__cursor.fetchall()
        text = result[0]
        self.__cursor.execute(
                '''
                SELECT DISTINCT tag FROM Relationship r
                        JOIN Tags t
                            ON t.id_tag = r.id_tag
                        JOIN Docs d 
                            ON r.id_doc = r.id_doc
                WHERE r.id_doc = ?
                ''', (id,)
            )
        tags = self.__clean_result__(self.__cursor.fetchall())
        return (text[0], tags)
    
    def find_doc_by_tag(self, tags):
        self.__cursor.execute(
                '''
                    SELECT d.id_doc, d.doc
                    FROM Docs d
                        JOIN Relationship r ON d.id_doc = r.id_doc
                        JOIN Tags t ON r.id_tag = t.id_tag
                        WHERE t.tag IN (%s)
                        GROUP BY d.id_doc
                        HAVING COUNT(DISTINCT t.id_tag) = %d;
                ''' % (','.join([f'"{a}"' for a in tags]), len(tags))
        )
        result = [list(a) for a in self.__cursor.fetchall()]
        return result
    
    def find_doc_without_tag(self, tag):
        self.__cursor.execute(
                '''
                    SELECT DISTINCT d.id_doc, d.doc
                    FROM Docs d
                    LEFT JOIN Relationship r ON d.id_doc = r.id_doc
                    WHERE d.id_doc NOT IN (
                            SELECT rel.id_doc FROM Relationship rel
                            JOIN Tags t  ON rel.id_tag = t.id_tag
                            WHERE t.tag = "%s"
                    )
                ''' % (tag)
        )
        result = [list(a) for a in self.__cursor.fetchall()]
        return result
    
    def get_tags_info(self):
        self.__cursor.execute(
            '''
            SELECT r.id_tag, tag, COUNT(*) 
            FROM Relationship r
                JOIN Tags t ON r.id_tag = t.id_tag 
            GROUP BY r.id_tag
            '''
        )
        result_query = self.__cursor.fetchall()
        result = {}
        for s in result_query:
            result[s[1]] = {
                "id" : s[0],
                "document_count" : s[2]
            }
        return result
    
    def get_docs_info(self):
        self.__cursor.execute(
            '''
            SELECT r.id_doc, doc, COUNT(*) 
            FROM Relationship r
                JOIN Docs t ON r.id_doc = t.id_doc 
            GROUP BY r.id_doc
            '''
        )
        result_query = self.__cursor.fetchall()
        result = {}
        for s in result_query:
            result[s[1][:15]] = {
                "id" : s[0],
                "tag_count" : s[2]
            }
        return result
    
    def add_doc(self, doc, tags):
        self.__cursor.execute('INSERT INTO Docs (doc) VALUES (?)', (doc,))
        self.__cursor.execute('SELECT id_doc FROM Docs WHERE doc = ?', (doc,))
        result_query = self.__cursor.fetchall()
        id_doc = result_query[0][0]
        tags_id = self.add_tags(tags)
        for id_tag in tags_id:
            self.__cursor.execute('INSERT INTO Relationship (id_doc, id_tag) VALUES (?, ?)', (id_doc, id_tag))

    def add_tags(self, tags):
        result = []
        for tag in tags:
            self.__cursor.execute('SELECT id_tag FROM Tags WHERE tag = ?', (tag,))
            result_query = self.__cursor.fetchall()
            if(result_query == []):
                self.__cursor.execute('INSERT INTO Tags (tag) VALUES (?)', (tag,))
                self.__cursor.execute('SELECT id_tag FROM Tags WHERE tag = ?', (tag,))
                result_query = self.__cursor.fetchall()

            id = result_query[0][0]
            result.append(id)
        return result


    def __clean_result__(self, result):
        return [a[0] for a in result]

    def __del__(self):
        self.__connection.commit()
        self.__connection.close()