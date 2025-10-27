import React, { useState, useRef, useEffect } from 'react';
import { Input, Button, Card, Row, Col, Typography, Space, Avatar, Tag } from 'antd';
import { SendOutlined, RobotOutlined, UserOutlined, ThunderboltOutlined } from '@ant-design/icons';
import './AIAssistant.less';

const { Title, Paragraph } = Typography;

interface Message {
  text: string;
  sender: 'user' | 'bot';
  timestamp: Date;
}

const SUGGESTED_QUESTIONS = [
  "How many datanodes are in the cluster?",
  "What is the current state of the cluster?",
  "Are there any unhealthy containers?",
  "What is the storage usage of each datanode?",
  "How many open keys are there?",
  "Show me the pipeline status"
];

const AIAssistant: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const chatWindowRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    if (chatWindowRef.current) {
      chatWindowRef.current.scrollTop = chatWindowRef.current.scrollHeight;
    }
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isLoading]);

  const parseMarkdown = (text: string) => {
    // Split text by bold markers
    const parts = text.split(/(\*\*.*?\*\*)/g);
    
    return parts.map((part, index) => {
      if (part.startsWith('**') && part.endsWith('**')) {
        // Remove ** and make bold
        const boldText = part.slice(2, -2);
        return <strong key={index}>{boldText}</strong>;
      }
      // Handle line breaks
      return part.split('\n').map((line, i, arr) => (
        <React.Fragment key={`${index}-${i}`}>
          {line}
          {i < arr.length - 1 && <br />}
        </React.Fragment>
      ));
    });
  };

  const sendMessage = async (query: string) => {
    if (query.trim() === '') return;

    const userMessage: Message = { 
      text: query, 
      sender: 'user',
      timestamp: new Date()
    };
    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      const response = await fetch('http://localhost:5001/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      if (!response.ok) {
        throw new Error('Network response was not ok');
      }

      const data = await response.json();
      const botMessage: Message = { 
        text: data.response, 
        sender: 'bot',
        timestamp: new Date()
      };
      setMessages(prev => [...prev, botMessage]);
    } catch (error) {
      const errorMessage: Message = { 
        text: 'Sorry, I encountered an error. Please try again.', 
        sender: 'bot',
        timestamp: new Date()
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSendMessage = () => {
    sendMessage(inputValue);
  };

  const handleSuggestionClick = (suggestion: string) => {
    sendMessage(suggestion);
  };

  return (
    <div className="ai-assistant-container">
      <div className="ai-assistant-header">
        <Space>
          <Avatar icon={<RobotOutlined />} size="large" className="assistant-avatar" />
          <div>
            <Title level={3} style={{ margin: 0 }}>AI Assistant</Title>
            <Paragraph type="secondary" style={{ margin: 0, fontSize: '12px' }}>
              Ask questions about your Ozone cluster
            </Paragraph>
          </div>
        </Space>
      </div>

      <Card className="chat-card" bordered={false}>
        <div className="chat-window" ref={chatWindowRef}>
          {messages.length === 0 && (
            <div className="empty-state">
              <RobotOutlined className="empty-icon" />
              <Title level={4}>Welcome to Ozone AI Assistant</Title>
              <Paragraph type="secondary" style={{ marginBottom: '24px' }}>
                I can help you understand your cluster's health, datanodes, containers, and more.
              </Paragraph>
              <div className="suggestions-container">
                <Space direction="vertical" size="small" style={{ width: '100%' }}>
                  <Title level={5} style={{ marginBottom: '12px', color: '#667eea' }}>
                    <ThunderboltOutlined /> Quick Questions
                  </Title>
                  <Space size={[8, 8]} wrap>
                    {SUGGESTED_QUESTIONS.map((question, index) => (
                      <Tag
                        key={index}
                        className="suggestion-tag"
                        onClick={() => handleSuggestionClick(question)}
                      >
                        {question}
                      </Tag>
                    ))}
                  </Space>
                </Space>
              </div>
            </div>
          )}
          
          {messages.map((message, index) => (
            <div key={index} className={`message-wrapper ${message.sender}`}>
              <div className="message-content">
                {message.sender === 'bot' && (
                  <Avatar 
                    icon={<RobotOutlined />} 
                    className="message-avatar bot-avatar"
                    size="small"
                  />
                )}
                <div className={`message-bubble ${message.sender}`}>
                  <div className="message-text">{parseMarkdown(message.text)}</div>
                </div>
                {message.sender === 'user' && (
                  <Avatar 
                    icon={<UserOutlined />} 
                    className="message-avatar user-avatar"
                    size="small"
                  />
                )}
              </div>
            </div>
          ))}
          
          {isLoading && (
            <div className="message-wrapper bot">
              <div className="message-content">
                <Avatar 
                  icon={<RobotOutlined />} 
                  className="message-avatar bot-avatar"
                  size="small"
                />
                <div className="typing-indicator">
                  <span></span>
                  <span></span>
                  <span></span>
                </div>
              </div>
            </div>
          )}
          
          <div ref={messagesEndRef} />
        </div>

        <div className="input-container">
          <Row gutter={12} align="middle">
            <Col flex="auto">
              <Input
                value={inputValue}
                onChange={e => setInputValue(e.target.value)}
                onPressEnter={handleSendMessage}
                placeholder="Ask a question about your Ozone cluster..."
                disabled={isLoading}
                size="large"
                className="chat-input"
              />
            </Col>
            <Col>
              <Button
                type="primary"
                icon={<SendOutlined />}
                onClick={handleSendMessage}
                loading={isLoading}
                size="large"
                className="send-button"
              >
                Send
              </Button>
            </Col>
          </Row>
        </div>
      </Card>
    </div>
  );
};

export default AIAssistant;




