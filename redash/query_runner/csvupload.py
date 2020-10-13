import os
import uuid
import csv

import xlrd


from redash import settings
from redash.query_runner import *
from redash.utils import json_dumps


def __extract_columns(sheet):
    ncols = sheet.ncols

    columns = []
    mapping_idx_to_name = {}

    for idx in range(ncols):
        column_name = sheet.cell_value(0, idx)
        if not column_name:
            column_name = 'column_{}'.format(idx)

        mapping_idx_to_name[idx] = column_name

        columns.append({
            'name': column_name,
            'friendly_name': column_name,
            'type': TYPE_STRING
        })

    return columns, mapping_idx_to_name


def __extract_data(sheet, columns, mapping_idx_to_name):
    ncols = sheet.ncols
    nrows = sheet.nrows

    rows = []
    col_data_type_flags = []

    for col_idx in range(0, ncols):
        col_data_type_flags.append(set())

    for row_idx in range(1, nrows):
        row = {}
        for col_idx in range(0, ncols):
            cell_v = sheet.cell_value(row_idx, col_idx)
            row[mapping_idx_to_name[col_idx]] = cell_v
            cell_type = guess_type(cell_v)
            col_data_type_flags[col_idx].add(cell_type)

        rows.append(row)

    for col_idx in range(0, ncols):
        if len(col_data_type_flags[col_idx]) == 1:
            columns[col_idx]['type'] = col_data_type_flags[col_idx].pop()
        elif len(col_data_type_flags[col_idx]) == 2 and TYPE_FLOAT in col_data_type_flags[
            col_idx] and TYPE_INTEGER in col_data_type_flags[col_idx]:
            columns[col_idx]['type'] = TYPE_FLOAT

    data = {'columns': columns, 'rows': rows}

    return data


def parse_excel(filename, idx=0):
    book = xlrd.open_workbook(filename)
    if idx > book.nsheets - 1:
        idx = 0
    sh = book.sheet_by_index(idx)
    columns, mapping_idx_to_name = __extract_columns(sh)
    data = __extract_data(sh, columns, mapping_idx_to_name)

    return data


def random():
    return str(uuid.uuid4())


def get_ext(filename):
    if '.' in filename:
        ext = filename.rsplit('.', 1)[1].lower()
        if ext in ["csv"]:
            return ext

    return None


def sheet_index(s):
    if ',' in s:
        arr = s.rsplit(',', 1)
        if arr[1].isdigit():
            return arr[0], int(arr[1])

    return s, 0


class CsvUpload(BaseQueryRunner):
    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
            }
        }

    @classmethod
    def annotate_query(cls):
        return False

    def test_connection(self):
        pass

    @classmethod
    def name(cls):
        return "Csv Upload"

    def run_query(self, query, user):
        try:
            filename = query.strip()
            if filename == "":
                return None, "Empty query"
            # print ("filename", filename)
            ext = get_ext(filename)
            # print ("ext", ext)
            if ext is None:
                return None, "Accepting only Csv files"

            path = os.path.abspath(os.path.join(settings.FILE_UPLOAD_FOLDER, filename))

            reader = csv.reader(open(path))


            line_num=0
            heads=[]
            bodys=[]
            for list in reader:
                if(line_num!=0):
                    bodys.append(list)
                else:
                    heads=list
                line_num+=1

            rows=[]
            columns=[]
            for head in heads:
                column={
                    'type': 'string',
                    'friendly_name': head.decode('utf-8'),
                    'name': head.decode('utf-8')
                }
                columns.append(column)

            for body in bodys:
                row={}
                for head in heads:
                    index = heads.index(head)
                    row[head]=body[index]
                rows.append(row)
            # print ("heads", heads)
            # print ("bodys", bodys)


            data = {'columns': columns, 'rows': rows}
            # return None, "done"
            return json_dumps(data), None
        except KeyboardInterrupt:
            return None, "Query cancelled by user."


class Csv(BaseHTTPQueryRunner):
    requires_url = False

    @classmethod
    def annotate_query(cls):
        return False

    def test_connection(self):
        pass

    def write_file(self, response, filename):
        with open(filename, 'wb') as output:
            output.write(response.content)

    def run_query(self, query, user):
        base_url = self.configuration.get("url", None)

        try:
            query = query.strip()

            if base_url is not None and base_url != "":
                if query.find("://") > -1:
                    return None, "Accepting only relative URLs to '%s'" % base_url

            if base_url is None:
                base_url = ""

            url = base_url + query
            url, idx = sheet_index(url)

            ext = get_ext(url)

            if ext is None:
                return None, "Accepting only Csv files"

            response, error = self.get_response(url)
            if error is not None:
                return None, error

            filename = random() + "." + ext

            self.write_file(response, filename)
            data = parse_excel(filename, idx)
            os.remove(filename)

            return json_dumps(data), None
        except KeyboardInterrupt:
            return None, "Query cancelled by user."


register(Csv)
register(CsvUpload)

# x = CsvUpload({})
# print(x.run_query(os.path.abspath(os.path.join("/workspace", "1.csv")), None))
