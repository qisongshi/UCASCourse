/*0, 2023E8013282124, shiqisong*/
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SSSP##name

#define EPS 1e-6
#define MAX_LENGTH 1e5

int m_vertex_id;

class VERTEX_CLASS_NAME(InputFormatter) : public InputFormatter
{
public:
    int64_t getVertexNum()
    {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex = n;
        return m_total_vertex;
    }
    int64_t getEdgeNum()
    {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge = n;
        return m_total_edge;
    }
    int getVertexValueSize()
    {
        m_n_value_size = sizeof(double);
        return m_n_value_size;
    }
    int getEdgeValueSize()
    {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize()
    {
        m_m_value_size = sizeof(double);
        return m_m_value_size;
    }
    void loadGraph()
    {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;

        double value = 1;
        int outdegree = 0;

        const char *line = getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld%lf", &from, &to, &weight);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i)
        {
            line = getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld%lf", &from, &to, &weight);
            if (last_vertex != from)
            {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            }
            else
            {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter) : public OutputFormatter
{
public:
    void writeResult()
    {
        int64_t vid;
        double value;
        char s[1024];

        for (ResultIterator r_iter; !r_iter.done(); r_iter.next())
        {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %f\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator) : public Aggregator<double>
{
public:
    void init()
    {
        m_global = 0;
        m_local = 0;
    }
    void *getGlobal()
    {
        return &m_global;
    }
    void setGlobal(const void *p)
    {
        m_global = *(double *)p;
    }
    void *getLocal()
    {
        return &m_local;
    }
    void merge(const void *p)
    {
        m_global += *(double *)p;
    }
    void accumulate(const void *p)
    {
        m_local += *(double *)p;
    }
};

class VERTEX_CLASS_NAME() : public Vertex<double, double, double>
{
public:
    void compute(MessageIterator *pmsgs)
    {
        double val;
        if (getSuperstep() == 0)
        {
            if (getVertexId() == m_vertex_id)
            {
                val = 0;
            }
            else
            {
                val = MAX_LENGTH;
            }
        }
        else
        {
            if (getSuperstep() >= 2)
            {
                double global_val = *(double *)getAggrGlobal(0);
                if (global_val < EPS)
                {
                    voteToHalt();
                    return;
                }
            }
            val = getValue();
            for (; !pmsgs->done(); pmsgs->next())
            {
                if (pmsgs->getValue() < val)
                {
                    val = pmsgs->getValue();
                }
            }

            double acc = fabs(getValue() - val);
            accumulateAggr(0, &acc);
        }
        *mutableValue() = val;
        OutEdgeIterator itr = getOutEdgeIterator();
        for (; !itr.done(); itr.next())
        {
            sendMessageTo(itr.target(), getValue() + itr.getValue());
        }
    }
};

class VERTEX_CLASS_NAME(Graph) : public Graph
{
public:
    VERTEX_CLASS_NAME(Aggregator) * aggregator;

public:
    void init(int argc, char *argv[])
    {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 4)
        {
            printf("Usage: %s <input path> <output path> <vertexid>\n", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        m_vertex_id = atoi(argv[3]);

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term()
    {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph *create_graph()
{
    Graph *pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph *pobject)
{
    delete (VERTEX_CLASS_NAME() *)(pobject->m_pver_base);
    delete (VERTEX_CLASS_NAME(OutputFormatter) *)(pobject->m_pout_formatter);
    delete (VERTEX_CLASS_NAME(InputFormatter) *)(pobject->m_pin_formatter);
    delete (VERTEX_CLASS_NAME(Graph) *)pobject;
}
