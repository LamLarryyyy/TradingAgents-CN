<template>
  <div class="foreign-sync-control">
    <el-card class="control-card" shadow="hover">
      <template #header>
        <div class="card-header">
          <el-icon class="header-icon"><Flag /></el-icon>
          <span class="header-title">港股/美股同步</span>
        </div>
      </template>

      <div class="control-content">
        <!-- 港股同步區塊 -->
        <div class="market-section">
          <div class="market-header">
            <span class="market-flag">🇭🇰</span>
            <span class="market-title">港股 (AKShare)</span>
            <el-tag :type="getStatusType(hkStatus?.status)" size="small">
              {{ getStatusText(hkStatus?.status) }}
            </el-tag>
          </div>
          
          <div v-if="hkStatus && hkStatus.status !== 'never_run'" class="market-stats">
            <div class="stat-row">
              <span class="stat-label">總數:</span>
              <span class="stat-value">{{ hkStatus.total || 0 }}</span>
              <span class="stat-label">新增:</span>
              <span class="stat-value success">{{ hkStatus.inserted || 0 }}</span>
              <span class="stat-label">更新:</span>
              <span class="stat-value primary">{{ hkStatus.updated || 0 }}</span>
            </div>
            <div v-if="hkStatus.finished_at" class="sync-time">
              完成時間: {{ formatTime(hkStatus.finished_at) }}
            </div>
          </div>
          
          <el-button
            type="primary"
            size="default"
            :loading="hkSyncing"
            @click="syncHK"
          >
            <el-icon><Refresh /></el-icon>
            同步港股
          </el-button>
        </div>

        <el-divider />

        <!-- 美股同步區塊 -->
        <div class="market-section">
          <div class="market-header">
            <span class="market-flag">🇺🇸</span>
            <span class="market-title">美股 (Alpha Vantage)</span>
            <el-tag :type="getStatusType(usStatus?.status)" size="small">
              {{ getStatusText(usStatus?.status) }}
            </el-tag>
          </div>
          
          <div v-if="usStatus && usStatus.status !== 'never_run'" class="market-stats">
            <div class="stat-row">
              <span class="stat-label">總數:</span>
              <span class="stat-value">{{ usStatus.total || 0 }}</span>
              <span class="stat-label">新增:</span>
              <span class="stat-value success">{{ usStatus.inserted || 0 }}</span>
              <span class="stat-label">更新:</span>
              <span class="stat-value primary">{{ usStatus.updated || 0 }}</span>
            </div>
            <div v-if="usStatus.finished_at" class="sync-time">
              完成時間: {{ formatTime(usStatus.finished_at) }}
            </div>
          </div>
          
          <el-button
            type="primary"
            size="default"
            :loading="usSyncing"
            @click="syncUS"
          >
            <el-icon><Refresh /></el-icon>
            同步美股
          </el-button>
        </div>

        <el-divider />

        <!-- 一鍵同步所有 -->
        <div class="sync-all-section">
          <el-button
            type="success"
            size="large"
            :loading="allSyncing"
            @click="syncAll"
          >
            <el-icon><Refresh /></el-icon>
            一鍵同步港股+美股
          </el-button>
        </div>
      </div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh, Flag } from '@element-plus/icons-vue'
import request from '@/api/request'

interface SyncStatus {
  job?: string
  status?: string
  total?: number
  inserted?: number
  updated?: number
  errors?: number
  finished_at?: string
  message?: string
}


const hkStatus = ref<SyncStatus | null>(null)
const usStatus = ref<SyncStatus | null>(null)
const hkSyncing = ref(false)
const usSyncing = ref(false)
const allSyncing = ref(false)

const getStatusType = (status?: string) => {
  switch (status) {
    case 'completed': return 'success'
    case 'running': return 'warning'
    case 'failed': return 'danger'
    case 'never_run': return 'info'
    default: return 'info'
  }
}

const getStatusText = (status?: string) => {
  switch (status) {
    case 'completed': return '已完成'
    case 'running': return '同步中'
    case 'failed': return '失敗'
    case 'never_run': return '未執行'
    default: return '未知'
  }
}

const formatTime = (time?: string) => {
  if (!time) return ''
  const date = new Date(time)
  return date.toLocaleString('zh-TW')
}

const fetchHKStatus = async () => {
  try {
    const res: any = await request.get('/api/sync/multi-source/hk/status')
    if (res.success) {
      hkStatus.value = res.data
    }
  } catch (err) {
    console.error('獲取港股狀態失敗:', err)
  }
}

const fetchUSStatus = async () => {
  try {
    const res: any = await request.get('/api/sync/multi-source/us/status')
    if (res.success) {
      usStatus.value = res.data
    }
  } catch (err) {
    console.error('獲取美股狀態失敗:', err)
  }
}

const syncHK = async () => {
  try {
    hkSyncing.value = true
    ElMessage.info('開始同步港股數據...')
    const res: any = await request.post('/api/sync/multi-source/hk/sync', { force: false })
    if (res.success) {
      hkStatus.value = res.data
      ElMessage.success(`港股同步完成: ${res.data.total} 支`)
    } else {
      ElMessage.error(`港股同步失敗: ${res.message}`)
    }
  } catch (err: any) {
    ElMessage.error(`港股同步失敗: ${err.message}`)
  } finally {
    hkSyncing.value = false
  }
}

const syncUS = async () => {
  try {
    usSyncing.value = true
    ElMessage.info('開始同步美股數據...')
    const res: any = await request.post('/api/sync/multi-source/us/sync', { force: false })
    if (res.success) {
      usStatus.value = res.data
      ElMessage.success(`美股同步完成: ${res.data.total} 支`)
    } else {
      ElMessage.error(`美股同步失敗: ${res.message}`)
    }
  } catch (err: any) {
    ElMessage.error(`美股同步失敗: ${err.message}`)
  } finally {
    usSyncing.value = false
  }
}

const syncAll = async () => {
  try {
    allSyncing.value = true
    ElMessage.info('開始同步港股和美股數據...')
    const res: any = await request.post('/api/sync/multi-source/foreign/sync', { force: false })
    if (res.success) {
      hkStatus.value = res.data.hk
      usStatus.value = res.data.us
      ElMessage.success('港股和美股同步完成')
    } else {
      ElMessage.error(`同步失敗: ${res.message}`)
    }
  } catch (err: any) {
    ElMessage.error(`同步失敗: ${err.message}`)
  } finally {
    allSyncing.value = false
  }
}

onMounted(() => {
  fetchHKStatus()
  fetchUSStatus()
})
</script>

<style scoped lang="scss">
.foreign-sync-control {
  .control-card {
    border-radius: 12px;
  }

  .card-header {
    display: flex;
    align-items: center;
    gap: 8px;
    
    .header-icon {
      font-size: 20px;
      color: var(--el-color-primary);
    }
    
    .header-title {
      font-size: 16px;
      font-weight: 600;
    }
  }

  .market-section {
    margin-bottom: 16px;
    
    .market-header {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 12px;
      
      .market-flag {
        font-size: 24px;
      }
      
      .market-title {
        font-size: 15px;
        font-weight: 500;
        flex: 1;
      }
    }
    
    .market-stats {
      background: var(--el-fill-color-light);
      padding: 12px;
      border-radius: 8px;
      margin-bottom: 12px;
      
      .stat-row {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        
        .stat-label {
          color: var(--el-text-color-secondary);
        }
        
        .stat-value {
          font-weight: 600;
          margin-right: 8px;
          
          &.success {
            color: var(--el-color-success);
          }
          
          &.primary {
            color: var(--el-color-primary);
          }
        }
      }
      
      .sync-time {
        margin-top: 8px;
        font-size: 12px;
        color: var(--el-text-color-secondary);
      }
    }
  }

  .sync-all-section {
    text-align: center;
    padding-top: 8px;
  }
}
</style>
