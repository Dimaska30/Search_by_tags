import sqlite3
import generate_data as Generator

class My_DB:
    
    def __init__(self):
        self.__connection = sqlite3.connect('my_database.db')
        self.__cursor = self.__connection.cursor()
        self.docs_n = 0
        self.tag_info = {}
        self.__create_doc_tables__()

    
    def __create_doc_tables__(self):
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
            CREATE TABLE IF NOT EXISTS Tags (
                id_tag INTEGER PRIMARY KEY,
                tag TEXT NOT NULL
            )
            '''
        )
        self.__cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS Relationship (
                id_tag INTEGER REFERENCES Tags(id_tag) ON UPDATE CASCADE,
                id_doc INTEGER REFERENCES Docs(id_doc) ON UPDATE CASCADE,
                PRIMARY KEY(id_tag, id_doc)
            )
            '''
        )
        self.__cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS AntiTags (
                tag INTEGER NOT NULL REFERENCES Tags(id_tag) ON UPDATE CASCADE,
                anti_tag INTEGER NOT NULL REFERENCES Tags(id_tag) ON UPDATE CASCADE,
                PRIMARY KEY(tag, anti_tag)
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
            words = Generator.generate_tags()
            my_pair_dict = Generator.generate_relationship(sentances, words)
            for doc, tags in my_pair_dict.items():
                self.add_doc(doc, tags)
        else:
            self.tag_info = self.__create_info_tags__()
            self.docs_n = len(docs)
            


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
        print(self.docs_n / 2, self.tag_info[tag]["document_count"])
        if (self.docs_n / 2 > self.tag_info[tag]["document_count"]):
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
        else:
            return self.find_doc_by_tag("!NOT!"+tag)
        
    
    def get_tags_info(self):
        return self.tag_info

    def __create_info_tags__(self):
        self.__cursor.execute(
            '''
            SELECT r.id_tag, tag, COUNT(*) 
            FROM Relationship r
                JOIN Tags t ON r.id_tag = t.id_tag 
                WHERE t.tag NOT LIKE "!NOT!%"
            GROUP BY r.id_tag
            '''
        )
        result_query = self.__cursor.fetchall()
        result = {}
        for s in result_query:
            if s[1][:5] != "!NOT!":
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
                JOIN Docs d ON r.id_doc = d.id_doc
                JOIN Tags t ON r.id_tag = t.id_tag
                WHERE t.tag NOT LIKE "!NOT!%" 
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
        tags_id = self.get_tags_id(tags, id_doc)
        self.__supplemening__(id_doc, tags)
        for id_tag, tag_name in zip(tags_id, tags):
            self.__add_relationship__(id_doc, id_tag)
            if tag_name in self.tag_info.keys():
                self.tag_info[tag_name]["document_count"] += 1
            else:
                self.tag_info[tag_name] = {
                    'id': id_tag,
                    'document_count': 1
                }
        self.docs_n += 1
    
    def __supplemening__(self, id_doc, tags):
        self.__cursor.execute('SELECT id_tag FROM Tags WHERE (tag NOT IN (%s))' % (",".join([f'"{a}"' for a in tags]))+" AND (tag NOT LIKE '!NOT!%')")
        result = self.__clean_result__(self.__cursor.fetchall())
        if(result != []):
            for r in result:
                anti_tag_id = self.__get_anti_tag__(r)
                self.__cursor.execute('INSERT INTO Relationship (id_doc, id_tag) VALUES (?, ?)', (id_doc, anti_tag_id))

    def get_tags_id(self, tags, id_doc):
        result = []
        for tag in tags:
            id = self.get_or_add_tag(tag, id_doc)
            result.append(id)
        return result
    
    def get_or_add_tag(self, tag, id_doc):
        self.__cursor.execute('SELECT id_tag FROM Tags WHERE tag = ?', (tag,))
        result_query = self.__cursor.fetchall()
        id = None
        if(result_query == []):
            id = self.__add_tag__(tag)
        else:
            id = result_query[0][0]
        return id

    def __add_tag__(self, tag):
        if (tag[:5] != "!NOT!"):
            self.__cursor.execute('INSERT INTO Tags (tag) VALUES (?)', (tag,))
            id_not_tag = self.__add_NOT_tag__(tag)
            self.__cursor.execute('SELECT id_tag FROM Tags WHERE tag = "%s"'%(tag))
            result_query = self.__cursor.fetchall()
            id = result_query[0][0]
            self.__cursor.execute('INSERT INTO AntiTags (tag, anti_tag) VALUES (?, ?)', (id,id_not_tag))
            return id
        else:
            raise ValueError("Тег не может начинаться с '!NOT!'")

    def delete_tag(self, tag):
        pass

    def __add_NOT_tag__(self, tag):
        not_tag = "!NOT!" + tag
        self.__cursor.execute('INSERT INTO Tags (tag) VALUES (?)', (not_tag,))
        self.__cursor.execute('SELECT id_tag FROM Tags WHERE tag = "%s"'%(not_tag))
        id_not_tag = self.__cursor.fetchall()[0][0]
        self.__cursor.execute('SELECT id_doc FROM Docs')
        id_docs = self.__clean_result__(self.__cursor.fetchall())
        for id in id_docs:
            self.__cursor.execute('INSERT INTO Relationship (id_doc, id_tag) VALUES (?, ?)', (id, id_not_tag))
        return id_not_tag

    def __add_relationship__(self, id_doc, id_tag):
        id_not_tag = self.__get_anti_tag__(id_tag)
        # Удаляем связку "документ"-"инти_тег"
        self.__cursor.execute('DELETE FROM Relationship WHERE id_tag = ? and id_doc = ?', (id_not_tag, id_doc))
        # Добавляем связку "документ"-"тег"
        self.__cursor.execute('INSERT INTO Relationship (id_doc, id_tag) VALUES (?, ?)', (id_doc, id_tag))

    def __get_anti_tag__(self, id_tag):
        self.__cursor.execute('SELECT anti_tag FROM AntiTags WHERE tag = ?', (id_tag,))
        result = self.__cursor.fetchall()
        id_not_tag = None
        if (result == []):
            tag = self.get_tag_name_by_id(id_tag)
            id_not_tag = self.__add_NOT_tag__(tag)
        else:
            id_not_tag = result[0][0]
        return id_not_tag


    def get_tag_name_by_id(self, id_tag):
        self.__cursor.execute('SELECT tag FROM Tags WHERE id_tag = ?', (id_tag,))
        result = self.__cursor.fetchall()
        if result == []:
            raise RuntimeError("Что-то пошло не так")
        else:
            return result[0][0]

    def __clean_result__(self, result):
        return [a[0] for a in result]
    
    def __delete_all__(self):
        self.__cursor.execute('DROP TABLE Docs;')
        self.__cursor.execute('DROP TABLE Tags;')
        self.__cursor.execute('DROP TABLE Relationship;')
        self.__cursor.execute('DROP TABLE AntiTags;')

    def __del__(self):
        self.__connection.commit()
        self.__connection.close()