class Project:
    def __init__(self, name=None, labels_per_task=0, icon_id="", metadata={}, model_id=None):
        self.name = name
        self.labels_per_task = labels_per_task
        self.icon_id = icon_id
        self.metadata = metadata
        self.model_id = ""
        if model_id:
            self.model_id = model_id

    def to_dic(self):
        dic = {}
        if self.name:
            dic["project_name"] = self.name
        if self.labels_per_task:
            dic["labels_per_task"] = self.labels_per_task
        if not self.metadata == {}:
            dic["metadata"] = self.metadata
        if not self.model_id == "":
            dic["model_id"] = self.model_id
        if not self.icon_id == "":
            dic["icon_id"] = self.icon_id
        return dic
