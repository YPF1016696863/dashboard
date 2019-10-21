import datetime
import logging
import os
import random

from flask import request
from flask_login import login_required

from redash import settings
from redash.apis import routes, json_response, json_response_with_status

logger = logging.getLogger(__name__)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in settings.FILE_UPLOAD_ALLOWED_EXTENSIONS


@routes.route('/api/file_upload', methods=['POST'])
@login_required
def upload_file():
    # check if the post request has the file part
    if 'file' not in request.files or request.files['file'] is None:
        return json_response_with_status({
            'error': 'NO_FILE_UPLOADED'
        }, 500)

    uploaded_file = request.files['file']

    if uploaded_file.filename == '':
        return json_response_with_status({
            'error': 'EMPTY_FILE_NAME'
        }, 500)

    if allowed_file(uploaded_file.filename):
        now = datetime.datetime.now()
        appended_file_name = os.path.join(str(now.year) + str(now.month), str(random.randint(100000, 999999)) + "__" + uploaded_file.filename)
        path = os.path.abspath(os.path.join(settings.FILE_UPLOAD_FOLDER, appended_file_name))

        parent_dir = os.path.dirname(path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)

        uploaded_file.save(path)

        return json_response({"status": "OK", "url": appended_file_name})
    else:
        return json_response_with_status({
            'error': 'NOT_ALLOWED_EXTENSIONS'
        }, 500)
