# https://stackoverflow.com/questions/3277503/how-to-read-a-file-line-by-line-into-a-list
def generate_word_file():
    """ Generate a large file containing words across 15 books. """
    text_files = [
                './textFiles/PrideAndPrejudice.txt', 
                './textFiles/ATaleOfTwoCities.txt', 
                './textFiles/AdventuresInWonderland.txt', 
                './textFiles/MobyDick.txt', 
                './textFiles/SherlockHolmes.txt', 
                './textFiles/Illiad.txt', 
                './textFiles/WarAndPeace.txt',
                './textFiles/DonQuixote.txt',
                './textFiles/BleakHouse.txt',
                './textFiles/Hunchback.txt',
                './textFiles/Brothers.txt',
                './textFiles/LesMis.txt',
                './textFiles/Middlemarch.txt',
                './textFiles/Dracula.txt',
                './textFiles/Ulysses.txt',                
                ]

    with open('./words.txt', 'w') as f:
        for file in text_files:
            with open(file, 'r') as f2:
                lines = [line.replace("\n", '') for line in f2.readlines()]
                for line in lines:
                    for word in line.split(' '):
                        word = word.strip('-\'\";\.,()”“?!_:_—\.’‘').lower()
                        if len(word) > 0:
                            f.write(f'{word}\n')
