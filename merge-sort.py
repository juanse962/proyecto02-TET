from multiprocessing import Process, Pipe
import time, random, sys

#Dependencies defined below main()

def main():
    """
    -Contiene la lsita a ordenar.
    -tiempo de ordenamiento secuencia.
    -tiempo de ordenamiento paralelo.
    """
    N = 500000
    if len(sys.argv) > 1: #el usuario ingresa un tamaño de lista.
        N = int(sys.argv[1])

    #Genera aleatoriamente el numero de N
    #Lista para ordenamientos mas pequeños se debe descomentar el print de abajo para verla.
    #lystbck = [1,3,4,2,9,7,6,8]
    lystbck = [random.random() for x in range(N)]

    #Merge-sort secuencial:
    lyst = list(lystbck)
    start = time.time()             #Tiempo de inicio
    lyst = mergesort(lyst)
    elapsed = time.time() - start   #Tiempo de fin

    if not isSorted(lyst):
        print('Sequential mergesort did not sort. oops.')
    
    print('Sequential mergesort: %f sec' % (elapsed))

    #Aquí que el uso de la CPU muestra una pausa.
    time.sleep(3)

    #Merge-sort paralelo 
    lyst = list(lystbck)
    start = time.time()
    n = 3 

    #Instancia el proceso y lo junta en una pipe
    pconn, cconn = Pipe()
    p = Process(target=mergeSortParallel, \
                args=(lyst, cconn, n))
    p.start()
    lyst = pconn.recv()
    #Bloquea hasta que se pueda recibir algo en el hilo
    
    p.join()
    elapsed = time.time() - start

    if not isSorted(lyst):
        print('Merge-sort en paralelo no tiene solucion')

    print('Parallel mergesort: %f sec' % (elapsed))


    time.sleep(3)
    
    lyst = list(lystbck)
    start = time.time()
    lyst = sorted(lyst)
    elapsed = time.time() - start


def merge(left, right):
    """realiza el ordenado"""
    ret = []
    li = ri = 0
    while li < len(left) and ri < len(right):
        if left[li] <= right[ri]:
            ret.append(left[li])
            li += 1
        else:
            ret.append(right[ri])
            ri += 1
    if li == len(left):
        ret.extend(right[ri:])
    else:
        ret.extend(left[li:])
    return ret

def mergesort(lyst):
    """
    Realiza la particion de la lista, devuelve una copia de la lista mas no
    la modifica
    """
    if len(lyst) <= 1:
        return lyst
    ind = len(lyst)//2
    return merge(mergesort(lyst[:ind]), mergesort(lyst[ind:]))


def mergeSortParallel(lyst, conn, procNum):
    """mergSortParallel recive una lista conectado por la pipe de arriba."""

    #Caso base, aquí mostramos un problema pequeño
    if procNum <= 0 or len(lyst) <= 1:
        conn.send(mergesort(lyst))
        conn.close()
        return

    ind = len(lyst)//2

    #creacion de procesos para ordenar las mitades de izquierda
    #y derecha de la lsita

    pconnLeft, cconnLeft = Pipe()
    leftProc = Process(target=mergeSortParallel, \
                       args=(lyst[:ind], cconnLeft, procNum - 1))

    pconnRight, cconnRight = Pipe()
    rightProc = Process(target=mergeSortParallel, \
                       args=(lyst[ind:], cconnRight, procNum - 1))

    leftProc.start()
    rightProc.start()

    conn.send(merge(pconnLeft.recv(), pconnRight.recv()))
    conn.close()

    #Join al proceso izquierdo y derecho.
    leftProc.join()
    rightProc.join()

def isSorted(lyst):
    """
    Verifica si esta ordenado el arreglo
    """

    for i in range(1, len(lyst)):
        if lyst[i] < lyst[i-1]:
            return False
    return True

if __name__ == '__main__':
    main()