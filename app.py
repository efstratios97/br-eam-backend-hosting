# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description: Main-Class
'''

from flask import Flask
from flask_cors import CORS
import Flask.Endpoints.DataManagerAPI as api_dm
import Flask.Endpoints.UserManagerAPI as api_um
import Flask.Endpoints.DataCleanserAPI as api_dc
import Flask.Endpoints.DataAnalyzerAPI as api_da
import Flask.Endpoints.DataHealthAPI as api_dh
import Flask.Endpoints.ArchitectureViewAPI as api_av


app = Flask(__name__)
CORS(app)

@app.route("/")
def helloWorld():
    return "<html>Hello says Project Athena</html>"

app.register_blueprint(api_dm.blueprint)
app.register_blueprint(api_um.blueprint)
app.register_blueprint(api_dc.blueprint)
app.register_blueprint(api_da.blueprint)
app.register_blueprint(api_dh.blueprint)
app.register_blueprint(api_av.blueprint)
    


#if __name__ == "__main__":
#    BRapp.run(host='0.0.0.0')


# def create_app():
#     BRapp = Flask(__name__)
#     CORS(BRapp)

#     # @BRapp.route("/")
#     # def helloWorld():
#     #     return "<html>Hello says Project Athena</html>"

#     BRapp.register_blueprint(api_dm.blueprint)
#     BRapp.register_blueprint(api_um.blueprint)
#     BRapp.register_blueprint(api_dc.blueprint)
#     BRapp.register_blueprint(api_da.blueprint)
#     BRapp.register_blueprint(api_dh.blueprint)
#     BRapp.register_blueprint(api_av.blueprint)
#     BRapp.run(host='0.0.0.0')


# if __name__ == "__main__":
#     create_app()