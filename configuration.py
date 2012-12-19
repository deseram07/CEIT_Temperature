'''
Created on the Dec 06, 2012

@author: Buddhika De Seram
'''

import sys
import time
import signal
from nyamuk import nyamuk
import nyamuk.nyamuk_const as NC
import json

from config import config
import wx

[wxID_FRAME1, wxID_FRAME1BUTTON1, wxID_FRAME1BUTTON2, wxID_FRAME1LISTBOX1, 
] = [wx.NewId() for _init_ctrls in range(4)]


class configuration(wx.Frame):
    def __init__(self, parent, id):
        self.deletelevel = None
        self.currentlvl = None
        self.currentpi = None
        self.newfeild = None
        self.newsen = None
        self.newmote = None
        self.pi_list = []
        self.mote_list = []
        level_list = ["1","2","3","4"]
        wx.Frame.__init__(self,parent,id, 
                          'Temperature Sensor Config',(550,150), size=(240,370))
        panel = wx.Panel(self)
        
#        boxes
        wx.StaticBox(self,-1, "Motes and Base Stations", (4,20), size = (235,145))
#        list of available levels
        self.levellist = level_list
        self.lblpilist = wx.StaticText(self, label="Building Level", pos = (40, 45))
        self.setlevel = wx.ComboBox(self, pos = (140, 40), size = (50,-1),
                                     choices = level_list, style = wx.CB_DROPDOWN)
        self.Bind(wx.EVT_COMBOBOX, self.EvtComboBox, self.setlevel)
        
#        list of available pis 
        self.pilist = self.pi_list
        self.lblpilist = wx.StaticText(self, label="Base St", pos = (20, 70))
        self.editpilist = wx.ComboBox(self, pos=(10, 90), size=(70, -1), 
                                      choices=self.pilist, style = wx.CB_DROPDOWN)
        self.Bind(wx.EVT_COMBOBOX, self.Evt2ComboBox, self.editpilist)
        
#        list of available motes in a pi
        self.motelist = self.mote_list
        self.lblmotelist = wx.StaticText(self, label="Mote List", pos = (120, 70))
        self.editmotelist = wx.ComboBox(self, pos=(100, 90), size=(130, -1), 
                                      choices=self.motelist, style = wx.CB_DROPDOWN)
        self.Bind(wx.EVT_COMBOBOX, self.Evt3ComboBox, self.editmotelist)

#        edit window
#        mote id
        wx.StaticBox(self, -1,"Add Mote:", (4, 165), size=(235,110))
        self.lbladdmoteid = wx.StaticText(self, label = "Mote ID", pos = (10,185))
        self.addmoteid = wx.TextCtrl(self, pos = (10,205), size = (60,-1))
        self.Bind(wx.EVT_TEXT, self.EvtText, self.addmoteid)
        
#        thingspeak id
        self.lbladdfield =  wx.StaticText(self, label = "Field No", pos = (90, 185))
        self.addfield = wx.TextCtrl(self,pos = (90,205), size = (60,-1))
        self.Bind(wx.EVT_TEXT, self.EvtText2, self.addfield)
        
#        sense id
        self.lbladdfield =  wx.StaticText(self, label = "Sen.se ID", pos = (170, 185))
        self.addsen = wx.TextCtrl(self,pos = (170,205), size = (60,-1))
        self.Bind(wx.EVT_TEXT, self.EvtText3, self.addsen)
        
#        register pi
        self.addpi = wx.StaticBox(self,-1, label = "Add Base Station:", pos = (4, 275), size=(235,90))
        self.lbllevellist = wx.StaticText(self, label = "Building Level", pos = (40, 300))        
        self.setlevel = wx.ComboBox(self, pos = (140, 295), size = (50,-1),
                                     choices = level_list, style = wx.CB_DROPDOWN)
        self.Bind(wx.EVT_COMBOBOX, self.Evt4ComboBox, self.setlevel)
        
#        buttons
#        delete mote
        self.delmote = wx.Button(self, label = "Delete Mote", pos = (140,125))
        self.Bind(wx.EVT_BUTTON, self.deletemote, self.delmote)
        
#        delete pi
        self.delpi = wx.Button(self, label = "Delete Base St", pos = (10,125))
        self.Bind(wx.EVT_BUTTON, self.deletepi, self.delpi)
        
#        add mote
        self.addmote = wx.Button(self, label = "Add", pos = (70,240))
        self.Bind(wx.EVT_BUTTON, self.add_mote, self.addmote)
        
#        add pi
        self.addpi = wx.Button(self, label = "Add", pos = (70,330))
        self.Bind(wx.EVT_BUTTON, self.add_pi, self.addpi)

#        close window
        self.Bind(wx.EVT_CLOSE, self.closewindow)
 
    
    def EvtComboBox(self, event):
        self.currentlvl = event.GetString();
        print self.currentlvl
        self.editpilist.SetValue('')
        self.editmotelist.SetValue('')
#        subscribe to topic and get data
        pionlevel_topic = config.topic_level + self.currentlvl
        topic = str(pionlevel_topic)
        self.connect()
        data = self.subscribe(topic)
        data = str(data)
        self.editpilist.Clear()
        self.editmotelist.SetValue('')
        self.editmotelist.Clear()
        self.editmotelist.SetValue('')
        try:
            pilist = json.loads(data)
            pis  = pilist["pi"]
            for i in pis:
                print i["id"]
                self.editpilist.Append(str(i["id"]))
                
        except ValueError:
            if (str(data) == "None"):
                wx.MessageBox("No base station on current level", "Info", wx.OK)
                self.editpilist.SetValue('')
                self.currentpi = None
            else:
                wx.MessageBox("Error with data", "Error", wx.OK)
        
    def Evt2ComboBox(self, event):
        self.currentpi = event.GetString()
#        subscribe to the current level data
        pionlevel_topic = config.topic_level + self.currentlvl
        topic = str(pionlevel_topic)
        self.connect()
        data = self.subscribe(topic)
        data = str(data)
        try:
            pilist = json.loads(data)
            pis  = pilist["pi"]
            for i in pis:
                if str(i['id']) == str(self.currentpi):
                    motes = i['mote']
                if len(motes) == 0:
                        print "No motes present"
                        self.editmotelist.Clear()
                        self.editmotelist.SetValue('')
                else:
                    for i in motes:
                        data = str(i['id']) + ', ' + str(i['TS']) + ', ' + str(i['sen'])
                        self.editmotelist.Append(data)
                break 
        except ValueError:
            wx.MessageBox("Error with data", "Error", wx.OK)
        
    def Evt3ComboBox(self, event):
        self.moteselection = event.GetString() 
        print 'Evt3ComboBox:', event.GetString() 
               
    
    def Evt4ComboBox(self, event):
        self.deletelevel = event.GetString() 
        print 'Evt4ComboBox:', event.GetString()
        
    def EvtText(self,event):
        self.newmote = event.GetString()
        print 'Evttext1:', event.GetString()
    
    def EvtText2(self,event):
        self.newfeild = event.GetString()
        print 'Evttext2:', event.GetString()
    
    def EvtText3(self,event):
        self.newsen = event.GetString()
        print 'Evttext3:', event.GetString()
        
    def deletemote(self,event):
        print self.currentpi
        print "deleted mote"
        if self.currentlvl != None:
            pionlevel_topic = config.topic_level + self.currentlvl
            topic = str(pionlevel_topic)
            self.connect()
            data = self.subscribe(topic)
            data = str(data)
            deleted = 0
            try:
                pilist = json.loads(data)
                count1 = 0
                count2 = 0
                for i in pilist['pi']:
                    if str(i['id']) == str(self.currentpi):
                        for j in i['mote']:
                            if (str(j['id'])+', '+str(j['TS']) + ', ' + str(j['sen'])) ==  str(self.moteselection):
                                msg = wx.MessageDialog(self,"Are you sure you want to delete current mote", "Detele Mote", wx.YES_NO)
                                result = msg.ShowModal()
                                if result == wx.ID_YES:
                                    pilist['pi'][count1]['mote'].pop(count2)
                                    print self.mote_list
                                deleted = 1
                                break
                            count2 += 1
                    count1 += 1
                    if deleted:
                        break
            except ValueError:
                wx.MessageBox("Error with data", "Error", wx.OK)
            data = json.dumps(pilist)
            self.publish(topic, data, True)
            self.editmotelist.SetValue('')
        else:
            wx.MessageBox("You need to select a mote to delete!", "Error", wx.OK)
            
    def deletepi(self,event):
        print "deleted pi"
#        deletes the specified pi and renames all the pis
        if self.currentlvl != None:
            pionlevel_topic = config.topic_level + self.currentlvl
            print "pilist", pionlevel_topic
            topic = str(pionlevel_topic)
            self.connect()
            print self.currentpi
            data = self.subscribe(topic)
            data = str(data)
            try:
                pilist = json.loads(data)
                count = 0
                for i in pilist['pi']:
                    if str(i['id']) == self.currentpi:
                        msg = wx.MessageDialog(self,"Are you sure you want to delete current pi", "Detele PI", wx.YES_NO)
                        result = msg.ShowModal()
                        if result == wx.ID_YES:
                            pilist['pi'].pop(count)
                        break
                    count += 1
            except ValueError:
                wx.MessageBox("Error with data", "Error", wx.OK)
            data = json.dumps(pilist)
            self.publish(topic, data, True)
            self.editpilist.SetValue('')
        else:
            wx.MessageBox("You need to select a base station to delete!", "Error", wx.OK)
            
    def add_mote(self,event):
        if self.newfeild is not None and self.newmote is not None and self.currentpi is not None and self.newsen is not None:
            pionlevel_topic = config.topic_level + self.currentlvl
            print "pilist", pionlevel_topic
            topic = str(pionlevel_topic)
            self.connect()
            data = self.subscribe(topic)
            data = str(data)
            count1 = 0
            print data
            try:
                pilist = json.loads(data)
                print pilist
                for i in pilist['pi']:
                    if str(i["id"]) == str(self.currentpi):
                        new_mote = config.mote_id
                        new_mote['id'] = str(self.newmote)
                        new_mote['TS'] = str(self.newfeild)
                        new_mote['sen'] = str(self.newsen)
                        dup = self.check(new_mote)
                        print dup
                        if dup[0] == 0 and dup[1] == 0 and dup[2] == 0:
                            msg = wx.MessageDialog(self,
                                                   "Are you sure you want to add \nMote id:\t" + new_mote['id']+ "\nThingspeak id:\t" + new_mote['TS'] + "\nSen.se id:\t" + new_mote['sen'],
                                                    "Add Mote", wx.YES_NO)
                            result = msg.ShowModal()
                            if result == wx.ID_YES:
                                pilist['pi'][count1]['mote'].append(new_mote)
                            break
                        else:
                            if dup[0] > 0:
                                wx.MessageBox("Mote id already exists", "Error", wx.OK)
                            elif dup[1] > 0:
                                wx.MessageBox("Thingspeak id already exists", "Error", wx.OK)
                            elif dup[2] > 0:
                                wx.MessageBox("Sen.se id already exists", "Error", wx.OK)
                            break
                    count1 += 1
                print pilist
                data = json.dumps(pilist)
                self.publish(topic, data, True)
            except ValueError:
                wx.MessageBox("Error with data", "Error", wx.OK)
        else:
            wx.MessageBox("No motes selected", "Info", wx.OK)
        self.addmoteid.SetValue('')
        self.addfield.SetValue('')
        self.addsen.SetValue('')
     
    def check(self, mote):
        for i in range(1,5):
            pionlevel_topic = config.topic_level + str(i)
            topic = str(pionlevel_topic)
            self.connect()
            data = self.subscribe(topic)
            data = str(data)
            dupid = 0
            dupts = 0
            dupsen = 0
            try:
                pilist = json.loads(data)
                pis  = pilist["pi"]
                print pis
                for i in pis:
                    motes = i['mote']
                    for j in motes:
                        if str(mote['id'])==str(j['id']):
                            dupid += 1
                            break
                        elif str(mote['TS'])==str(j['TS']):
                            dupts += 1
                            break
                        elif str(mote['sen'])==str(j['sen']):
                            dupsen += 1
                    if dupid>0 or dupts>0 or dupsen>0:
                        break
            except ValueError:
                print "Error"
            if dupid>0 or dupts>0 or dupsen>0:
                break
        return [dupid, dupts, dupsen]
    
    def add_pi(self,event):
        if self.deletelevel != None:
            pionlevel_topic = config.topic_level + self.deletelevel
            print "pilist", pionlevel_topic
            topic = str(pionlevel_topic)
            self.connect()
            data = self.subscribe(topic)
            data = str(data)
            try:
                pilist = json.loads(data)
                print pilist
                x  = len(pilist["pi"])
                newpi_id = config.newpi_id
                if x == 0:
                    pi_no = int(str(self.deletelevel) + "01")
                    newpi_id["id"] = pi_no
                else:
                    pi_id= int(pilist["pi"][x-1]["id"]) + 1
                    newpi_id["id"] = pi_id
                pilist["pi"].append(newpi_id)
                data = json.dumps(pilist)
                self.publish(topic, data, True)
            except ValueError:
                pilist = str(data)
    #            this is the case when there is no pi's present
                if data == "None":
                    data = config.new_pi
                    new_pi = json.loads(data)
                    pi_no = int(str(self.deletelevel) + "01")
                    new_pi["pi"][0]["id"] = pi_no
                    data = json.dumps(new_pi)
                    self.publish(topic, data, True)
                else:
                    print "Error reading pilist"
        else:
            wx.MessageBox("You need to select a level to add a Base station", "Info", wx.OK)
        
    def closewindow(self, event):
        self.Destroy()
        sys.exit(0)
    
    def connect(self):
        while 1:
            config.np = nyamuk.Nyamuk(config.client_pub, server = config.server)
            config.ns = nyamuk.Nyamuk(config.client_sub, server = config.server)
            rs = config.ns.connect()
            rp = config.np.connect()
            if (rs != NC.ERR_SUCCESS) or (rp != NC.ERR_SUCCESS):
                print "Can't connect"
                time.sleep(30)
            else:
                print "Connection successful"
                return
        
    def publish(self,topic, data, Retain):
        start = time.time()
        while 1:
            if (config.np.publish(topic, data, retain = Retain) == 0):
                print "published = " + data
                break
            if (time.time() - start > 30):
                print "publish failed, Reconnecting"
                start = self.connect()
        config.np.loop()

    def subscribe(self, topic):
        while 1:
            rc = config.ns.subscribe(topic, 0)
            if rc == NC.ERR_SUCCESS:
                ev = config.ns.pop_event()
                if ev != None: 
                    if ev.type == NC.CMD_PUBLISH:
                        payload = ev.msg.payload
                        if payload is not None and str(ev.msg.topic) == topic:
                            ev = None
                            break
                    elif ev.type == 1000:
                        print "Network Error. Msg = ", ev.msg
                rc = config.ns.loop()
        return payload
    
if __name__=='__main__':
    app = wx.PySimpleApp()
    frame = configuration(parent=None, id = 1)
    frame.Show()
    app.MainLoop()
    
