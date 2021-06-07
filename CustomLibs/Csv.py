import csv
import xlsxwriter
def read_csv_file(file_name):
    data = []
    with open(file_name,  "rt", encoding="utf8") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    return data

def write_file(file_name, text):
    with open(file_name, "w", encoding="utf8") as f:
        f.write(text)
        f.close()

def write_csv_file(file_name, text1, text2, text3):
    with open(file_name, mode='a', encoding="utf8") as csv_file:
        csvfile_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvfile_writer.writerow([text1, text2, text3])
        csv_file.close()

def write_csv_file_2(file_name, text1, text2, text3, text4, text5):
    with open(file_name, mode='a', encoding="utf8") as csv_file:
        csvfile_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvfile_writer.writerow([text1, text2, text3, text4, text5])
        csv_file.close()
def write_list_to_csv_file(file_name, *text):
    with open(file_name, mode='a', encoding="utf8") as csv_file:
        csvfile_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvfile_writer.writerow(text[1], text[2], text[3])
        csv_file.close()

def write_excel_file(row, text1, text2, text3):
    # Create an new Excel file and add a worksheet.
    workbook = xlsxwriter.Workbook('demo.xlsx')
    worksheet = workbook.add_worksheet()


    # Widen the first column to make the text clearer.
    # worksheet.set_column('A:A', 20)

    # Add a bold format to use to highlight cells.
    #bold = workbook.add_format({'bold': True})

    # Write some simple text.
    #worksheet.write('A1', 'Hello')

    # Text with formatting.
    #worksheet.write('A2', 'World', bold)

    # Write some numbers, with row/column notation.
    worksheet.write(int(row), 0, text1)
    worksheet.write(int(row), 1, text2)
    worksheet.write(int(row), 2, text3)


    #worksheet.write(3, 0, 123.456)

    # Insert an image.
    #worksheet.insert_image('B5', 'logo.png')

    workbook.close()

