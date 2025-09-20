# # ... [Other imports remain unchanged] ...
# from features.timeview import TimeViewFeature
# from features.fftview import FFTViewFeature
# from features.waterfall import WaterfallFeature
# from features.orbit import OrbitFeature
# # ... [Other code remains unchanged] ...

# class DashboardWindow(QWidget):
#     # ... [init and setup methods unchanged] ...

#     def ondatareceived(self, featurename, tagname, modelname, values, samplerate, frameindex):
#         try:
#             # Always update all feature instances upon data receipt
#             for key, featureinstance in self.featureinstances.items():
#                 instancefeature, instancemodel, instancechannel, _ = key
#                 if instancemodel != modelname:
#                     continue
#                 mappedfeatures = getattr(self.mqtthandler, "featuremapping", {}).get(featurename, [featurename])
#                 if instancefeature not in mappedfeatures and instancefeature != featurename:
#                     continue
#                 # Passes data to features regardless of active subwindow
#                 QTimer.singleShot(0, lambda fi=featureinstance: fi.ondatareceived(tagname, modelname, values, samplerate, frameindex))
#         except Exception as e:
#             logging.error(f"Error in ondatareceived for {featurename} {modelname}, frame {frameindex}: {e}")
#             self.console.appendtoconsole(f"Error processing data for {featurename}: {e}")

#     # ... [Other methods remain unchanged] ...



# class FFTViewFeature:
#     # ... [__init__, settings, and UI code unchanged] ...

#     def ondatareceived(self, tagname, modelname, values, samplerate, frameindex):
#         if self.modelname != modelname or self.channelindex is None:
#             return
#         if len(values) == 0:
#             return
#         try:
#             self.lastframeindex = frameindex
#             # Accept and plot new data as soon as received
#             channeldata = values[self.channelindex] if isinstance(values[0], (list, np.ndarray)) else values
#             self.samplerate = samplerate if samplerate > 0 else 1000
#             scalingfactor = 3.3 / 65535.0
#             rawdata = np.array(channeldata[:self.maxsamples], dtype=np.float32)
#             self.latestdata = rawdata * scalingfactor
#             self.databuffer.append(self.latestdata.copy())
#             if len(self.databuffer) > self.settings.numberofaverages:
#                 self.databuffer = self.databuffer[-self.settings.numberofaverages:]
#             self.updateplot()  # ENSURE PLOTTING HERE
#         except Exception as e:
#             self.logandsetstatus(f"Error in ondatareceived, frame {frameindex}: {e}")

#     # ... [rest unchanged] ...



#     class WaterfallFeature:
#     # ... [__init__, helpers, and UI code unchanged] ...

#     def ondatareceived(self, tagname, modelname, values, samplerate, frameindex):
#         if self.modelname != modelname:
#             return
#         if len(values) == 0:
#             return
#         try:
#             self.lastframeindex = frameindex
#             totalchannels = len(values)
#             if totalchannels != self.channelcount:
#                 self.channelcount = totalchannels
#                 self.channelnames = [f"Channel{i+1}" for i in range(self.channelcount)]
#                 self.datahistory = [[] for _ in range(self.channelcount)]
#                 self.phasehistory = [[] for _ in range(self.channelcount)]
#             channeldata = values
#             self.samplerate = samplerate if samplerate > 0 else 4096
#             self.samplesperchannel = len(channeldata[0]) if channeldata and len(channeldata[0]) > 0 else 4096
#             # (Rest of feature FFT logic unchanged)
#             self.updatewaterfallplot(filteredfrequencies)  # ENSURE PLOTTING HERE
#         except Exception as e:
#             if self.console:
#                 self.console.appendtoconsole(f"WaterfallFeature Error processing data, frame {frameindex}: {e}")

#     # ... [rest unchanged] ...

