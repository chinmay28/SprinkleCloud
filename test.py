CHUNK_SIZE = 1024 * 64
file_number = 1
with open('test_folder/src_file.pdf', 'rb') as src_file:
    chunk = src_file.read(CHUNK_SIZE)
    while chunk:
        with open('test_folder/pdf_file_part_' + str(file_number), 'wb') as chunk_file:
            chunk_file.write(chunk)
        file_number += 1
        chunk = src_file.read(CHUNK_SIZE)

with open('test_folder/dest_file.pdf', 'wb') as dest_file:
    for i in range(1, 9):
        with open('test_folder/pdf_file_part_{}'.format(i), 'rb') as chunk_file:
            chunk = chunk_file.read()
            dest_file.write(chunk)

