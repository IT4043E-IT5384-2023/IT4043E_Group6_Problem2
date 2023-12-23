import os,sys,time,random,argparse

def obtain_args():
    parser=argparse.ArgumentParser(description='Big data project config',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config', default='config/model.yaml',type=str, help='The path to the model configuration')
    args = parser.parse_args()
    return args
