# Relational Algebra 

## Translate Queries

### Assume a schema: R(a,b,c), S(x,y)
- select * from R
    - Res(a,b,c) = R
- select a,b from R
    - Res(a,b) = Proj[a,b]R
- select * from R where a > 5
    - Res(a,b,c) = Sel[a > 5]R
- select * from R join S on R.a = S.y
    - Res(a,b,c,x,y) = R Join[a=y] S
- select * from R join S on R.a = S.y where R.b < 2 -> 
    - Res(a,b,c,x,y) = (Sel[b < 2]R) Join[a=y] S
    - Res(a,b,c,x,y) = S Join[a=y] (Sel[b < 2]R)
    - Res(a,b,c,x,y) = Sel[b < 2] (S Join[a=y] R)

    --
    select * from R where a > 5;

    Sel[a>5](R)


    --
    select * from R where id = 1234 and a > 5;

    Sel[id=1234 & a>5](R)

    Sel[a>5](Sel[id=1234](R))



    --
    select R.a from R, S where R.id = S.j;
    select R.a from R join S on R.id = S.j;

    Proj[a](R join S)   or   Proj[a](S join R)

    Proj[a](Proj[a,id](R) Join Proj[j](S))

    Proj[a](R Join Proj[j](S))

    Proj[a](R Join sort(proj[j](S)))   if R sorted, then sort-merge join


    --
    select * from R,S where R.id=S.j and R.a=6

    Sel[a=6](R Join S)

    ((Sel[a=6]R) Join S)
    (S Join (Sel[a=6]R))


    --
    select R.a from R, S, T where R.id = S.j and S.k = T.y;

    Proj[a]((R Join S) Join T)

    Proj[a](R Join (S Join T))

    Proj[a]((S Join T) Join R)

    Proj[a]((T Join S) Join R)

    Proj[a](S Join[R.id = S.j & S.k = T.y] (R X T)) NOT useful

## Cost merge sort 

Cost = 2N * (# of passes)

Buffers = b 
Pages = p
passes = ciel(logb-1(pages/b)) + 1

E.g., with 5 buffer pages, to sort 108 page file:
Pass 0: = 22 sorted runs of 5 pages each (last run is only 3 pages)
Pass 1: = 6 sorted runs of 20 pages each (last run is only 8 pages)
Pass 2: 2 sorted runs, 80 pages and 28 pages
Pass 3: Sorted file of 108 pages


# Storage

## 