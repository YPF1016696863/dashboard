import datetime
import logging
import os
import random

from flask import request , Response
from flask_login import login_required

from redash import settings
from redash.apis import routes, json_response, json_response_with_status

logger = logging.getLogger(__name__)

# IMAGE STUFF
# IMAGE_UPLOAD_FOLDER = fix_assets_path("../uploads/image")
# IMAGE_UPLOAD_ALLOWED_EXTENSIONS = set(['jpg', 'png', 'jpge'])
# IMAGE_UPLOAD_MAX_CONTENT_LENGTH = 16 * 1024 * 1024
# IMAGE_ALLOWED_EXTENSIONS = set(['jpg', 'png', 'jpge'])
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in settings.IMAGE_UPLOAD_ALLOWED_EXTENSIONS


@routes.route('/api/image_upload', methods=['POST'])
# @login_required
def upload_image():
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
        path = os.path.abspath(os.path.join(settings.IMAGE_UPLOAD_FOLDER, appended_file_name))

        parent_dir = os.path.dirname(path)
        print(parent_dir)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)

        uploaded_file.save(path)
        print(path)

        # print("1111111111")
        # with open(r'///opt/redash/image_uploads/{}'.format(appended_file_name),'rb') as f:
        #     image=f.read()
        #     resp = Response(image, mimetype="image/jpg") 
        # print(image)
        # print(resp)
        # return resp

        return json_response({"status": "OK", "url": appended_file_name})
    else:
        return json_response_with_status({
            'error': 'NOT_ALLOWED_EXTENSIONS'
        }, 500)

