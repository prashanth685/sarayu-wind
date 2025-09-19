                                # elif main_channels == 10:
                                #     # For 10 channels, assume interleaved as ADC1 (CH1-CH5) + ADC2 (CH6-CH10)
                                #     adc1_data = total_values[:samples_per_channel * 5]
                                #     adc2_data = total_values[samples_per_channel * 5:samples_per_channel * 10]
                                #     for i in range(0, len(adc1_data), 5):
                                #         for ch in range(5):
                                #             if i + ch < len(adc1_data):
                                #                 channel_data[ch].append(adc1_data[i + ch])
                                #     for i in range(0, len(adc2_data), 5):
                                #         for ch in range(5):
                                #             if i + ch < len(adc2_data):
                                #                 channel_data[ch + 5].append(adc2_data[i + ch])