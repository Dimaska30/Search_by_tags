from DB import My_DB 

my_DB = My_DB()

#my_DB.get_cursor().execute("DROP TABLE Relationship;")

# my_DB.get_cursor().execute("SELECT * FROM AntiTags")

# print(my_DB.get_cursor().fetchall())

# my_DB.get_cursor().execute('''SELECT * FROM Relationship r
#                                     JOIN Tags t ON r.id_tag = t.id_tag
#                                     ''')

# print(my_DB.get_cursor().fetchall())

print("Информация о тегах:")
tags = my_DB.get_tags_info()
big_tag = None
temp = 0
for k, v in tags.items():
    print(v["id"], f"'{k}'.", "Кол-во документов: "+str(v["document_count"]))
    if v["document_count"] > temp:
        big_tag = k
        temp = v["document_count"]
print('\n')

print("Информация о документах с тегами:")
docs = my_DB.get_docs_info()
big_doc_id = None
temp = 0
for k, v in docs.items():
    print(v["id"], f"'{k}'.", "Кол-во тегов: "+str(v["tag_count"]))
    if v["tag_count"] > temp:
        big_doc_id = v["id"]
        temp = v["tag_count"]
print('\n')

print(f"Посмотрим информацию о {big_doc_id} документе:")
doc_any = my_DB.get_doc_info(big_doc_id)
print("Текст:")
print(doc_any[0])
print("Теги:")
print(doc_any[1])
print('\n')

print(f"Найдем документы с такими же тэгами, как у {big_doc_id} документа:")
result = my_DB.find_doc_by_tag(doc_any[1])
for d in result:
    print(f"id: {d[0]}. Text: {d[1][:35]}")
print('\n')

print(f"Найдем документы c тегом '{big_tag}':")
result = my_DB.find_doc_by_tag([big_tag])
for d in result:
    print(f"id: {d[0]}. Text: {d[1][:35]}")
print("Кол-во документов:", len(result))
print('\n')


print(f"Найдем документы, которые не содкержат тег '{big_tag}':")
t = tags[big_tag]['document_count']
print(f"Их кол-во должно быть 50 - {t} = {50 - t}")
result = my_DB.find_doc_without_tag(big_tag)
print(f"Найдено: {len(result)}")
for d in result:
    print(f"id: {d[0]}. Text: {d[1][:35]}")
print('\n')
print("end")
