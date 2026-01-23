'use client'

import { useState } from 'react'
import { PageLayout } from '@/components/layout/PageLayout'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'
import toast from 'react-hot-toast'
import { Save, Eye, EyeOff } from 'lucide-react'

export default function SettingsPage() {
  const [showApiKeys, setShowApiKeys] = useState(false)
  const [settings, setSettings] = useState({
    approvalTimeout: '30',
    notificationsEnabled: true,
    autoRefresh: true,
  })

  const handleSave = () => {
    toast.success('Settings saved successfully')
  }

  const maskApiKey = (key: string) => {
    if (!showApiKeys) {
      return '••••••••••••••••••••'
    }
    return key
  }

  return (
    <PageLayout title="Settings" subtitle="Configure system settings">
      <div className="space-y-6 max-w-4xl">
        {/* General Settings */}
        <Card variant="bordered">
          <CardHeader>
            <CardTitle>General Settings</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Select
              label="Approval Timeout (minutes)"
              options={[
                { value: '15', label: '15 minutes' },
                { value: '30', label: '30 minutes' },
                { value: '60', label: '1 hour' },
                { value: '120', label: '2 hours' },
              ]}
              value={settings.approvalTimeout}
              onChange={(e) =>
                setSettings({ ...settings, approvalTimeout: e.target.value })
              }
            />

            <div className="flex items-center justify-between py-2">
              <div>
                <p className="font-medium text-gray-900">Enable Notifications</p>
                <p className="text-sm text-gray-500">Receive browser notifications for important events</p>
              </div>
              <label className="relative inline-flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  className="sr-only peer"
                  checked={settings.notificationsEnabled}
                  onChange={(e) =>
                    setSettings({ ...settings, notificationsEnabled: e.target.checked })
                  }
                />
                <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
              </label>
            </div>

            <div className="flex items-center justify-between py-2">
              <div>
                <p className="font-medium text-gray-900">Auto Refresh</p>
                <p className="text-sm text-gray-500">Automatically refresh data</p>
              </div>
              <label className="relative inline-flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  className="sr-only peer"
                  checked={settings.autoRefresh}
                  onChange={(e) =>
                    setSettings({ ...settings, autoRefresh: e.target.checked })
                  }
                />
                <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
              </label>
            </div>
          </CardContent>
        </Card>

        {/* API Configuration */}
        <Card variant="bordered">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>API Configuration</CardTitle>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowApiKeys(!showApiKeys)}
              >
                {showApiKeys ? (
                  <>
                    <EyeOff className="h-4 w-4 mr-2" />
                    Hide
                  </>
                ) : (
                  <>
                    <Eye className="h-4 w-4 mr-2" />
                    Show
                  </>
                )}
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            <Input
              label="OpenAI API Key"
              value={maskApiKey('sk-...ABC123')}
              disabled
            />
            <Input
              label="Anthropic API Key"
              value={maskApiKey('sk-ant-...XYZ789')}
              disabled
            />
            <Input
              label="LangFuse API Key"
              value={maskApiKey('pk_lf_...DEF456')}
              disabled
            />
          </CardContent>
        </Card>

        {/* Agent Control */}
        <Card variant="bordered">
          <CardHeader>
            <CardTitle>Agent Control</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {[
                'ServiceNow Agent',
                'Jira Agent',
                'GitHub Agent',
                'Infrastructure Agent',
                'Data Agent',
                'GCP Monitor Agent',
              ].map((agent) => (
                <div
                  key={agent}
                  className="flex items-center justify-between p-3 border border-gray-200 rounded-lg"
                >
                  <div className="flex items-center gap-3">
                    <span className="font-medium text-gray-900">{agent}</span>
                    <Badge variant="success">Enabled</Badge>
                  </div>
                  <label className="relative inline-flex items-center cursor-pointer">
                    <input type="checkbox" className="sr-only peer" checked readOnly />
                    <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                  </label>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Save Button */}
        <div className="flex justify-end">
          <Button onClick={handleSave} size="lg">
            <Save className="h-4 w-4 mr-2" />
            Save Settings
          </Button>
        </div>
      </div>
    </PageLayout>
  )
}
