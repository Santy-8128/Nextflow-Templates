#!/shared/opt/nextflow/nextflow-20.01.0-all

Channel.from('book1','book2','book3','book4','book5').set{Book}

process SplitBook{
    input: val(book) from Book
    output: tuple val(book), file("chapter.*") into ChapterList

    shell:
    '''
    #Split Book into Chapters
    touch chapter.{1..10}
    '''
}


ChapterList
.map{ tuple( groupKey(it[0],it[1].size()) , it[1]) }
.transpose()
.map{ tuple( it[0], it[0]+"."+it[1].name.toString().tokenize('.').get(1) , it[1]) }
.set{Chapters}

process SplitChapter{
    input:  tuple val(book), val(chapter), file(chapterFile)    from Chapters
    output: tuple val(book), val(chapter), file("paragraph.*")  into ParagraphList

    shell:
    '''
    #Split Chapter into Paragraphs
    for i in {1..10}; do
        head -c 1k < /dev/urandom > paragraph.$i
    done
    '''
}


ParagraphList
.map{ tuple( it[0], groupKey(it[1],it[2].size()) , it[2]) }
.transpose()
.set{Paragraphs}

process CountWordsPerParagraph{
    input:  tuple val(book), val(chapter), file(paragraphFile) from Paragraphs
    output: tuple val(book), val(chapter), file("paragraphCount") into ParagraphCount

    shell:
    '''
    #Count words in each paragraph
    wc -l < !{paragraphFile} > paragraphCount
    sleep 0.1s
    '''
}

ParagraphCount
.groupTuple(by:1)
.map{ tuple( it[0][0], it[1], it[2] )}
.set{ParagraphCountReduced}


process CountWordsPerChapter{
    input:  tuple val(book), val(chapter), file("paragraphCount.?") from ParagraphCountReduced
    output: tuple val(book), file("ChapterCount") into ChapterCount

    shell:
    '''
    #Count words in each book
    cat paragraphCount* | awk '{s+=$1}END{print s}' > ChapterCount
    sleep 1s
    '''
}


ChapterCount
.groupTuple(by:0)
.map{ tuple( it[0], it[1] )}
.set{ChapterCountReduced}


process CountWordsPerBook{
    input:  tuple val(book), file("ChapterCount.?") from ChapterCountReduced
    output: tuple val(book), env(result) into BookCount

    shell:
    '''
    #Count words in each book
    result=$(cat ChapterCount* | awk '{s+=$1}END{print s}')
    '''
}

BookCount.view()
