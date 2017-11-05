import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
FLAGS = None

def print_args():
    try: 
        dic = FLAGS.__flags
    except:
        dic = vars(FLAGS)#vars(args)
    keys = sorted(dic.keys())
    print("\ncommon flags:")
    for i in keys:
        if dic[i]:
            print('{:>23} {:>23}'.format(i, str(dic[i])))
    print()


def main(_):
    print_args()
    if FLAGS.comp == "mobike":
        from comps.mobike.run import run_sol
    print("run competition %s solution %s"%(FLAGS.comp,FLAGS.sol))
    run_sol(FLAGS)

if __name__ == "__main__":
    from flags import get_parser
    parser = get_parser()
    args = parser.parse_args()
    FLAGS = args
    main(None)
