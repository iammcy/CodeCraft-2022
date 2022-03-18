/**
 * @file CodeCraft-2022.cpp
 * @author iammcy
 * @version 0.3
 * @date 2022-03-15
 * 
 * @copyright Copyright (c) 2022
 * 
 */
#include <bits/stdc++.h>
using namespace std;
#define Debug true

// 输入数据文件夹
#if Debug
const string DATA_DIR = "../data/";
#else
const string DATA_DIR = "/data/";
#endif

// 输出数据文件夹
#if Debug
const string OUTPUT_DIR = "../data/";
#else
const string OUTPUT_DIR = "/output/";
#endif

typedef long long ll;
struct degree
{
    int idx = 0;
    int num = 0;
};

bool cmp(const degree & d1, const degree & d2){
    return d1.num < d2.num;
}

class Scheduler
{
private:
    /* data */
    int T = 0, M = 0, N = 0, qos_constraint;
    vector<string> clients, sites;
    vector<vector<ll>> demands;
    vector<ll> site_used;
    vector<ll> site_bw;
    vector<vector<int>> qos;
    vector<degree> client_indegree;
    vector<degree> sites_outdegree;
    vector<vector<float>> weight;
    vector<vector<ll>> ans;
    
public:
    Scheduler(/* args */);
    ~Scheduler();
    void input();
    void output(int time);
    void scheduling();
    void adjust_weight();
};

Scheduler::Scheduler(/* args */)
{
    /**
     * @brief 
     * @param data 
     * @return  
     */
}

Scheduler::~Scheduler()
{
}

void Scheduler::input()
{
    string line, field;
    // 1.读取demand.csv
    ifstream inFile1(DATA_DIR + "demand.csv", ios::in);
    if (!inFile1){
        cout<<"failed to open file"<<endl;
        exit(1);
    }

    // 1.1.读取表头
    getline(inFile1, line);
    line = line.substr(0, line.size()-1);
    istringstream sin(line);
    getline(sin, field, ',');
    while (getline(sin, field, ','))
    {
        clients.push_back(field);
        M++;
    }
    
    // 1.2.读取客户节点带宽需求
    while (getline(inFile1, line))
    {
        istringstream sin(line);
        vector<ll> tmp(M);
        int i = 0;
        getline(sin, field, ',');
        while (getline(sin, field, ','))
        {
            tmp[i] = atoll(field.c_str());
            i++;
        }
        demands.push_back(tmp);
        T++;
    }

    inFile1.close();    

    // 4.读取site_bandwidth.csv
    ifstream inFile4(DATA_DIR + "site_bandwidth.csv", ios::in);
    if (!inFile4){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    getline(inFile4, line);
    while(getline(inFile4, line)){
        istringstream sin(line);
        getline(sin, field, ',');
        sites.push_back(field);
        getline(sin, field, ',');
        ll bw = atoll(field.c_str());
        site_bw.push_back(bw);
        N++;
    }
    inFile4.close();

    // 2.读取config.ini
    ifstream inFile2(DATA_DIR + "config.ini", ios::in);
    if (!inFile2){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    getline(inFile2, line);
    getline(inFile2, line);
    istringstream conf_sin(line);
    getline(conf_sin, field, '=');
    getline(conf_sin, field, '=');
    qos_constraint = atoi(field.c_str());
    inFile2.close();

    // 3.读取qos.csv
    client_indegree = vector<degree>(M);
    for (int i=0; i<M; i++){
        client_indegree[i].idx = i;
    }
    sites_outdegree = vector<degree>(N);
    for (int i=0; i<N; i++){
        sites_outdegree[i].idx = i;
    }
    ifstream inFile3(DATA_DIR + "qos.csv", ios::in);
    if (!inFile3){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    getline(inFile3, line);
    int j = 0;
    while (getline(inFile3, line))
    {
        istringstream sin(line);
        vector<int> tmp(M, 0);
        int i = 0;
        getline(sin, field, ',');
        while (getline(sin, field, ','))
        {
            int x = atoi(field.c_str());
            if (x < qos_constraint){
                tmp[i] = 1;
                client_indegree[i].num++;
                sites_outdegree[j].num++;
            }
            i++;
        }
        qos.push_back(tmp);
        j++;
    }
    inFile3.close();

}

void Scheduler::adjust_weight()
{
    // 预处理客户节点的分配权重
    weight = vector<vector<float>>(N, vector<float>(M, 0));
    vector<float> sum(M, 0);
    for (int i=0; i<N; i++){
        float avg_bw = (float)(site_bw[i] - site_used[i]) / (float)sites_outdegree[i].num;
        for (int j=0; j<M; j++){
            if (qos[i][j] == 1)
            {
                weight[i][j] = avg_bw;
                sum[j] += avg_bw;
            }
        }
    }
    for (int i=0; i<N; i++){
        for (int j=0; j<M; j++){
            if (qos[i][j] == 1)
            {
                weight[i][j] = weight[i][j] / sum[j];
            }
        }
    }
}

void Scheduler::scheduling()
{
    // 预处理客户节点优先级
    sort(client_indegree.begin(), client_indegree.end(), cmp);

    // 预处理边缘节点优先级
    // sort(sites_outdegree.begin(), sites_outdegree.end(), cmp);

    // 遍历所有时刻生成方案
    for (int i = 0; i < T; i++)
    {
        // 初始化每个时刻的分配方案
        ans = vector<vector<ll>>(N, vector<ll>(M, 0));
        site_used = vector<ll>(N, 0);

        for (int j = 0; j < M; j++)
        {
            int client_idx = client_indegree[j].idx;

            // 对该客户节点分配流量
            while (demands[i][client_idx])
            {
                this->adjust_weight();
                ll rest_demands = demands[i][client_idx];

                // 遍历所有边缘节点
                for (int k = 0; k < N; k++)
                {                    
                    // 进入可分配边缘节点
                    if (qos[k][client_idx] == 1 && (site_bw[k] - site_used[k] > 0))
                    {
                        ll res = site_bw[k] - site_used[k];
                        ll weight_demands = (ll)((float)rest_demands * weight[k][client_idx] + 1);
                        if (weight_demands > demands[i][client_idx])
                            weight_demands = demands[i][client_idx];
                        
                        if (res < weight_demands)
                        {
                            site_used[k] =  site_bw[k];
                            ans[k][client_idx] += res;
                            demands[i][client_idx] -= res;
                        }
                        else{
                            site_used[k] += weight_demands;
                            ans[k][client_idx] += weight_demands;
                            demands[i][client_idx] -= weight_demands;
                        }
                    }

                }
            }
            
        }

        // 检查客户节点是否分配完
        for (int j = 0; j < M; j++)
        {
            if (demands[i][j])
            {
                cout<<"分配不合理"<<endl;
                exit(1);
            }
        }
        
        this->output(i);
    }
    
}

void Scheduler::output(int time)
{
    string res = "";
    ofstream outFile(OUTPUT_DIR + "solution.txt", ios::app);
    if (!outFile){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    for (int j=0; j<M; j++){
        res += clients[j] + ":";
        bool tag = false;
        for (int k=0; k<N; k++){
            if (ans[k][j] > 0){
                res += "<" + sites[k] + "," + to_string(ans[k][j]) + ">,";
                tag = true;
            }
        }
        if (tag)
            res = res.substr(0, res.size()-1);
        if (time != T-1 || j != M-1)
        {
            res += "\n";
        }
    }
    outFile<<res;
    outFile.close();
}


int main() {

#if Debug
    // 记录起始时间
    clock_t start, finish;
    start = clock();
#endif

    Scheduler scheduler;
    scheduler.input();
    scheduler.scheduling();

#if Debug
    // 记录结束时间
    finish = clock();
    cout<<(double)(finish - start) / CLOCKS_PER_SEC<<"s"<<endl;
#endif
	return 0;
}
