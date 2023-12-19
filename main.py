from DB import My_DB 

my_DB = My_DB()

#my_DB.get_cursor().execute("DROP TABLE Relationship;")

print("Информация о тегах:")
tags = my_DB.get_tags_info()
for k, v in tags.items():
    print(v["id"], f"'{k}'.", "Кол-во документов: "+str(v["document_count"]))
print('\n')

print("Информация о документах с тегами:")
docs = my_DB.get_docs_info()
for k, v in docs.items():
    print(v["id"], f"'{k}'.", "Кол-во тегов: "+str(v["tag_count"]))
print('\n')

print("Посмотрим информацию о 39 документе:")
doc_39 = my_DB.get_doc_info(39)
print("Текст:")
print(doc_39[0])
print("Теги:")
print(doc_39[1])
print('\n')

print("Найдем документы с такими же тэгами, как у 39 документа:")
result = my_DB.find_doc_by_tag(doc_39[1])
for d in result:
    print(f"id: {d[0]}. Text: {d[1][:35]}")
print('\n')

print("Найдем документы c тегом 'разделить':")
result = my_DB.find_doc_by_tag(["разделить"])
for d in result:
    print(f"id: {d[0]}. Text: {d[1][:35]}")
print("Кол-во документов:", len(result))
print('\n')


print("Найдем документы, которые не содкержат тег 'разделить':")
t = tags['разделить']['document_count']
print(f"Их кол-во должно быть 50 - {t} = {50 - t}")
result = my_DB.find_doc_without_tag("разделить")
print(f"Найдено: {len(result)}")
for d in result:
    print(f"id: {d[0]}. Text: {d[1][:35]}")
print('\n')
print("end")
