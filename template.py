import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QListView, QPushButton, QLabel,QLineEdit,QCheckBox,QListWidget,QListWidgetItem,QAbstractItemView
from PyQt5.uic import loadUi
from PyQt5 import QtGui as gui
from PyQt5 import QtCore as qq

class PredictionWindow(QMainWindow):

    def __init__(self):
        super(PredictionWindow, self).__init__()
        loadUi("template.ui", self)
        eval_btn = self.findChild(QPushButton, "eval_button")
        eval_btn.clicked.connect(self.predict_click)
        self.fill_actors()
        self.fill_directors()
        self.fill_language()

    def get_budget(self):
        box = self.findChild(QLineEdit,"budget_input")
        return box.text()

    def get_adult_check(self):
        box = self.findChild(QCheckBox,"adult_check")
        return True if box.isChecked() else False

    def get_collection_check(self):
        box = self.findChild(QCheckBox,"collection_check")
        return True if box.isChecked() else False

    def fill_actors(self):
        self.actors_list = self.findChild(QListView,"actors_list")
        self.actors_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.actors_list.addItem("Item 1")
        self.actors_list.addItem("Item 2")
        self.actors_list.addItem("Item 3")

    def get_actors(self):
        return [x.data() for x in self.actors_list.selectedIndexes()]



    def predict_click(self):
        #self.hide_prediction()
        selected = {}
        print(self.get_actors())
        print(self.get_directors())
        print(self.get_languages())
        print(self.get_budget())
        print(self.get_adult_check())
        print(self.get_collection_check())
        dic = {'actors':self.get_actors(),'directors':self.get_directors(),'languages':self.get_languages(),
               'budget':self.get_budget(),'adult':self.get_adult_check(),'collection':self.get_collection_check()}
        self.lbl = self.findChild(QLabel, "result_text")
        self.lbl.setText(str(dic))
        #callback(self.show_prediction, selected)

    def fill_list(self, list_name, items):
        list = self.findChild(QListView, list_name)
        model = gui.QStandardItemModel()
        for item in items:
            it = gui.QStandardItem(item)
            model.appendRow(it)
        list.setModel(model)
        return list

    def fill_language(self):
        sez = ["US","GER","SLO"]
        self.languages_list = self.fill_list("language_list",sez)

    def get_languages(self):
        return [x.data() for x in self.languages_list.selectedIndexes()]
    def fill_directors(self):
        self.director_list = self.findChild(QListView,"director_list")
        self.director_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.director_list.addItem("Item A")
        self.director_list.addItem("Item B")
        self.director_list.addItem("Item C")

    def get_directors(self):
        return [x.data() for x in self.director_list.selectedIndexes()]
if __name__ == "__main__":
    app = QApplication(sys.argv)
    entry = PredictionWindow()
    entry.show()

    sys.exit(app.exec_())