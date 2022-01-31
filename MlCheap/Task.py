from .consts import *


class Data:
    def __init__(self, name):
        self.name = name

    def __to_dic__(self):
        return {}

    def to_dic(self):
        return {'type': 'data', 'name': self.name, 'data': self.__to_dic__()}


class DataText(Data):
    def __init__(self, name, text):
        super(DataText, self).__init__(name)
        self.text = text

    def __to_dic__(self):
        return {'text': self.text}


class Label:
    def __init__(self, name):
        self.name = name

    def __to_dic__(self):
        return {}

    def to_dic(self):
        return {'type': 'label', 'name': self.name, 'meta-label': self.__to_dic__()}


class LabelTagging(Label):
    def __init__(self, name, question, all_tags, preferred_tags):
        super(LabelTagging, self).__init__(name)
        self.all_tags = all_tags
        self.question = question
        self.preferred_tags = preferred_tags

    def __to_dic__(self):
        return {'all-tags': self.all_tags,
                'question': self.question,
                'preferred-tags': self.preferred_tags}


class Task:
    def __init__(self, task_type, callback=None, unique_id=None):
        self.task_type = task_type
        self.callback = callback
        self.unique_id = unique_id

    def get_items(self):
        return []

    def to_dic(self):
        dic = {
            'task-type': self.task_type,
            'callback': self.callback,
            'items': self.get_items()
        }
        if self.unique_id:
            dic.update({'_id': self.unique_id})
        print(dic)
        return dic

    def set_id(self, unique_id):
        self.unique_id = unique_id


class Text_Tagging(Task):

    def __init__(self, text, question, all_tags, preferred_tags, callback=None, unique_id=None):
        super(Text_Tagging, self).__init__(TEXT_TAGGING_TYPE, callback, unique_id)
        self.data_text = DataText('text', text)
        self.label_tagging = LabelTagging('tagging', question, all_tags, preferred_tags)

    def get_items(self):
        return [self.data_text.to_dic(), self.label_tagging.to_dic()]

