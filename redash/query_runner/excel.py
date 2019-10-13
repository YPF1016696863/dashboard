import uuid
import os
import re

import xlrd

from redash.query_runner import *
from redash.utils import json_dumps


def random():
    return str(uuid.uuid4())

class Excel(BaseHTTPQueryRunner):
    requires_url = False

    @classmethod
    def annotate_query(cls):
        return False

    def test_connection(self):
        pass

    def write_file(self, response, filename):
        with open(filename, 'wb') as output:
            output.write(response.content)

    def extract_columns(self, sheet):
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

    def extract_data(self, sheet, columns, mapping_idx_to_name):
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

    def parse_excel(self, filename):
        book = xlrd.open_workbook(filename)
        sh = book.sheet_by_index(0)
        columns, mapping_idx_to_name = self.extract_columns(sh)
        data = self.extract_data(sh, columns, mapping_idx_to_name)

        return data

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

            response, error = self.get_response(url)
            if error is not None:
                return None, error

            match = re.search('.*?(\.xls\S?$)', url)
            if not match:
                return None, "Accepting only excel files"

            extension = match.group(1)

            if extension not in [".xls", ".xlsx", ".xlsm"]:
                return None, "Accepting only excel files"

            filename = random() + extension

            self.write_file(response, filename)
            data = self.parse_excel(filename)
            os.remove(filename)

            return json_dumps(data), None
        except KeyboardInterrupt:
            return None, "Query cancelled by user."


register(Excel)