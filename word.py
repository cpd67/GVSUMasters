def generate_word_file():
    """" Generate a large file containing words across 10 books. """"
    text_files = [
                './textFiles/PrideAndPrejudice.txt', 
                './textFiles/ATaleOfTwoCities.txt', 
                './textFiles/AdventuresInWonderland.txt', 
                './textFiles/MobyDick.txt', 
                './textFiles/SherlockHolmes.txt', 
                './textFiles/Illiad.txt', 
                './textFiles/WarAndPeace.txt', 
                './textFiles/SleepyHollow.txt,
                ]

    with open('./words.txt', 'w') as f2:
        for file in text_files:
            with open(file, 'r') as f:
                line = f.readline()
                while line != '':
                    words = line.replace("\n", '').split(' ')
                    for word in words:
                        if len(word) > 0:
                            word = word.strip('-\'\";\.,()”“?!_:_—\.’‘').lower()
                            f2.write(f'{word}\n')              
                    line = f.readline()
